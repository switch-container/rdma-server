// This is originated by fastswap rmserver !
#include <cassert>
#include <cstring>
#include <rdma/rdma_cma.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <sys/un.h>
#include <thread>

#include "pseudo-mm-rdma-server.h"
#include "scm.h"

int RDMAQueue::create_qp() {
  if (!this->client_cm_id) {
    fprintf(stderr, "client_cm_id not set for RDMAQueue\n");
    return 1;
  }
  struct ibv_qp_init_attr qp_attr = {};

  this->cq = srv->cq;
  qp_attr.send_cq = this->cq;
  qp_attr.recv_cq = this->cq;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = 10;
  qp_attr.cap.max_recv_wr = 10;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;

  if (REPORT_IF_NONZERO(rdma_create_qp(client_cm_id, srv->dev->pd, &qp_attr)))
    return 1;
  this->qp = this->client_cm_id->qp;
  return 0;
}

RDMAServer::RDMAServer(int queue_num_) : queue_num(queue_num_) {}

int RDMAServer::init(uint16_t port_num) {
  struct sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_num);

  if (REPORT_IF_ZERO(queues = new RDMAQueue[queue_num]))
    return 1;
  if (REPORT_IF_ZERO(buffer = new uint8_t[BUFFER_SIZE]))
    return 1;
  // if (REPORT_IF_NONZERO(fill_buffer_poll()))
  //   return 1;
  for (int i = 0; i < queue_num; i++) {
    queues[i].srv = this;
  }
  if (REPORT_IF_ZERO(ec = rdma_create_event_channel()))
    return 1;
  if (REPORT_IF_NONZERO(rdma_create_id(ec, &srv_cm_id, NULL, RDMA_PS_TCP)))
    return 1;
  if (REPORT_IF_NONZERO(rdma_bind_addr(srv_cm_id, (struct sockaddr *)&addr)))
    return 1;
  if (REPORT_IF_NONZERO(rdma_listen(srv_cm_id, queue_num + 1)))
    return 1;
  uint16_t port = ntohs(rdma_get_src_port(srv_cm_id));
  printf("RDMA server listening on port %d, queue num = %d.\n", port,
         queue_num);
  // start a server to load memory image
  std::thread t([this] {
    if (REPORT_IF_NONZERO(__init_buf_sock()))
      return;
    REPORT_IF_NONZERO(__serve_buf_sock());
  });
  t.detach();
  return 0;
}

int RDMAServer::connect_queue() {
  int ret;
  RDMAQueue *q;
  for (int i = 0; i < queue_num; i++) {
    q = &queues[i];
    while (q->state == RDMAQueue::INIT) {
      ret = __handle_one_event();
      if (ret) {
        printf("__handle_one_event failed when connect_queue()\n");
        return 1;
      }
    }
  }
  return __poll_for_mr_send();
}

int RDMAServer::serve() {
  int ret;
  while (__get_connected_queue_num() != 0) {
    ret = __handle_one_event();
    if (ret != 0 && ret != 123456) {
      fprintf(stderr, "error happend in __handle_one_event()\n");
    }
  }
  return 0;
}

int RDMAServer::__handle_one_event() {
  int ret;
  struct rdma_cm_event *event = nullptr;
  struct rdma_cm_event event_copy;
  ret = rdma_get_cm_event(ec, &event);
  if (ret) {
    fprintf(stderr, "Failed to retrieve a cm event, errno: %d \n", -errno);
    return -errno;
  }
  /* lets see, if it was a good event */
  if (event->status != 0) {
    fprintf(stderr, "CM event has non zero status: %d\n", event->status);
    ret = -(event->status);
    /* important, we acknowledge the event */
    rdma_ack_cm_event(event);
    return ret;
  }
  memcpy(&event_copy, event, sizeof(*event));
  rdma_ack_cm_event(event);
  printf("A new %s type event is received \n",
         rdma_event_str(event_copy.event));
  switch (event_copy.event) {
  case RDMA_CM_EVENT_CONNECT_REQUEST:
    return __handle_connect_request(event_copy.id, &event_copy.param.conn);
  case RDMA_CM_EVENT_ESTABLISHED:
    return __handle_establish(event_copy.id);
  case RDMA_CM_EVENT_DISCONNECTED:
    __handle_disconnect(event_copy.id);
    return 123456;
  default:
    printf("unknown event: %s\n", rdma_event_str(event_copy.event));
    return 1;
  }
}
int RDMAServer::__init_device(struct rdma_cm_id *client_cm_id) {
  if (dev) {
    // already initialized
    if (dev->verbs != client_cm_id->verbs) {
      fprintf(stderr, "found different verbs, unsupport!\n");
      return 1;
    }
    return 0;
  }
  if (REPORT_IF_ZERO(dev = new Device))
    return 1;
  dev->verbs = client_cm_id->verbs;
  if (REPORT_IF_ZERO(dev->pd = ibv_alloc_pd(client_cm_id->verbs)))
    return 1;
  if (REPORT_IF_ZERO(comp_channel =
                         ibv_create_comp_channel(client_cm_id->verbs)))
    return 1;
  /* Now we create a completion queue (CQ) where actual I/O
   * completion metadata is placed. The metadata is packed into a structure
   * called struct ibv_wc (wc = work completion). ibv_wc has detailed
   * information about the work completion. An I/O request in RDMA world
   * is called "work" ;)
   */
  if (REPORT_IF_ZERO(
          cq = ibv_create_cq(client_cm_id->verbs /* which device*/,
                             64 /* maximum capacity*/,
                             NULL /* user context, not used here */,
                             comp_channel /* which IO completion channel */,
                             0 /* signaling vector, not used here*/)))
    return 1;
  printf("Completion queue (CQ) is created at %p with %d elements \n", cq,
         cq->cqe);
  /* Ask for the event for all activities in the completion queue*/
  if (REPORT_IF_NONZERO(ibv_req_notify_cq(
          cq /* on which CQ */, 0 /* 0 = all event type, no filter*/)))
    return -errno;
  return 0;
}

int RDMAServer::__register_buffer_pool() {
  if (memory_region) // already registered
    return 0;
  memory_region = ibv_reg_mr(dev->pd, static_cast<void *>(buffer), BUFFER_SIZE,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ);
  if (REPORT_IF_ZERO(memory_region))
    return 1;
  return 0;
}

int RDMAServer::__handle_connect_request(struct rdma_cm_id *client_cm_id,
                                         struct rdma_conn_param *param) {

  struct rdma_conn_param cm_params = {};
  struct ibv_device_attr attrs = {};
  RDMAQueue *q = &queues[connected_queue++];

  if (REPORT_IF_ZERO(q->state == RDMAQueue::INIT))
    return 1;
  printf("%s\n", __FUNCTION__);

  client_cm_id->context = q;
  q->client_cm_id = client_cm_id;

  if (REPORT_IF_NONZERO(__init_device(client_cm_id)))
    return 1;
  if (REPORT_IF_NONZERO(__register_buffer_pool()))
    return 1;
  if (REPORT_IF_NONZERO(q->create_qp()))
    return 1;

  if (REPORT_IF_NONZERO(ibv_query_device(dev->verbs, &attrs)))
    return 1;

  printf("attrs: max_qp=%d, max_qp_wr=%d, max_cq=%d max_cqe=%d \
          max_qp_rd_atom=%d, max_qp_init_rd_atom=%d\n",
         attrs.max_qp, attrs.max_qp_wr, attrs.max_cq, attrs.max_cqe,
         attrs.max_qp_rd_atom, attrs.max_qp_init_rd_atom);

  printf("srv attrs: initiator_depth=%d responder_resources=%d\n",
         param->initiator_depth, param->responder_resources);

  // the following should hold for initiator_depth:
  // initiator_depth <= max_qp_init_rd_atom, and
  // initiator_depth <= param->initiator_depth
  cm_params.initiator_depth = param->initiator_depth;
  // the following should hold for responder_resources:
  // responder_resources <= max_qp_rd_atom, and
  // responder_resources >= param->responder_resources
  cm_params.responder_resources = param->responder_resources;
  cm_params.rnr_retry_count = param->rnr_retry_count;
  cm_params.flow_control = param->flow_control;

  if (REPORT_IF_NONZERO(rdma_accept(client_cm_id, &cm_params)))
    return 1;

  return 0;
}

int RDMAServer::__handle_establish(struct rdma_cm_id *client_cm_id) {
  RDMAQueue *q = static_cast<RDMAQueue *>(client_cm_id->context);
  if (q == nullptr) {
    fprintf(stderr, "client_cm_id->context not set in __handle_establish\n");
    return 1;
  }

  printf("%s\n", __FUNCTION__);

  if (q->state != RDMAQueue::INIT) {
    fprintf(stderr, "weird RDMAQueue states in %s\n", __FUNCTION__);
    return 1;
  }

  if (q == &queues[0]) {
    struct ibv_send_wr wr = {};
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge sge = {};
    struct Memregion servermr = {};

    printf("connected. sending memory region info.\n");
    printf("MR key=%u base vaddr=%p\n", memory_region->rkey,
           memory_region->addr);

    servermr.baseaddr = (uint64_t)memory_region->addr;
    servermr.key = memory_region->rkey;

    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

    sge.addr = (uint64_t)&servermr;
    sge.length = sizeof(servermr);

    if (REPORT_IF_NONZERO(ibv_post_send(q->qp, &wr, &bad_wr)))
      return 1;
    // TODO: poll here
  }

  q->state = RDMAQueue::CONNECTED;
  return 0;
}

int RDMAServer::__handle_disconnect(struct rdma_cm_id *client_cm_id) {
  RDMAQueue *q = static_cast<RDMAQueue *>(client_cm_id->context);
  printf("%s\n", __FUNCTION__);
  if (q == nullptr) {
    fprintf(stderr, "client_cm_id->context not set in __handle_establish\n");
    return 1;
  }

  if (q->state == RDMAQueue::CONNECTED) {
    q->state = RDMAQueue::INIT;
    rdma_destroy_qp(q->client_cm_id);
    rdma_destroy_id(q->client_cm_id);
  }
  return 0;
}

int RDMAServer::__get_connected_queue_num() const {
  int count = 0;
  for (int i = 0; i < queue_num; i++) {
    if (queues[i].state == RDMAQueue::CONNECTED)
      count++;
  }
  return count;
}

RDMAServer::~RDMAServer() {
  if (queues)
    delete[] queues;
  if (dev) {
    if (memory_region)
      ibv_dereg_mr(memory_region);
    ibv_dealloc_pd(dev->pd);
    delete dev;
  }
  if (buffer)
    delete[] buffer;
  if (cq)
    ibv_destroy_cq(cq);
  if (comp_channel)
    ibv_destroy_comp_channel(comp_channel);
  if (ec)
    rdma_destroy_event_channel(ec);
  if (srv_cm_id)
    rdma_destroy_id(srv_cm_id);
}

int RDMAServer::__poll_for_mr_send() {
  struct ibv_cq *cq_ptr = NULL;
  struct ibv_wc wc;
  void *context = NULL;
  int ret = -1, total_wc = 0;
  /* We wait for the notification on the CQ channel */
  if (REPORT_IF_NONZERO(ibv_get_cq_event(
          comp_channel, /* IO channel where we are expecting the
                         * notification
                         */
          &cq_ptr,      /* which CQ has an activity. This should be the same as
                           CQ we created before */
          &context)))   /* Associated CQ user context, which we did set */
    return -errno;
  /* Request for more notifications. */
  if (REPORT_IF_NONZERO(ibv_req_notify_cq(cq_ptr, 0)))
    return -errno;
  /* We got notification. We reap the work completion (WC) element. It is
   * unlikely but a good practice it write the CQ polling code that
   * can handle zero WCs. ibv_poll_cq can return zero. Same logic as
   * MUTEX conditional variables in pthread programming.
   */
  total_wc = 0;
  do {
    ret = ibv_poll_cq(cq_ptr /* the CQ, we got notification for */,
                      1 /* number of remaining WC elements*/,
                      &wc /* where to store */);
    if (ret < 0) {
      fprintf(stderr, "Failed to poll cq for wc due to %d \n", ret);
      /* ret is errno here */
      return ret;
    }
    total_wc += ret;
  } while (total_wc < 1);
  printf("send info to client finished\n");
  /* Now we check validity and status of I/O work completions */
  if (wc.status != IBV_WC_SUCCESS) {
    fprintf(stderr,
            "Work completion (WC) has error status: %d (means: %s) at\n",
            -wc.status, ibv_wc_status_str(wc.status));
    /* return negative value */
    return -(wc.status);
  }
  /* Similar to connection management events, we need to acknowledge CQ events
   */
  ibv_ack_cq_events(cq_ptr, 
		       1 /* we received one event notification. This is not 
		       number of WC elements */);
  return 0;
}

// only for test with kselftest
static inline char random_char(int i, unsigned long seed) {
  unsigned long tmp = i * seed + seed - 1;
  return tmp % 30 + 'a';
}

static void fill_single_page(void *start, unsigned long seed) {
  int i;
  char *iter;
  for (i = 0; i < PAGE_SIZE; i++) {
    iter = (char *)(start) + i;
    *iter = random_char(i, seed);
  }
}

int RDMAServer::fill_buffer_poll() {
  char *fill_start;
  for (unsigned long i = 0; i < (BUFFER_SIZE / PAGE_SIZE); i++) {
    fill_start = (char *)buffer + i * PAGE_SIZE;
    fill_single_page(fill_start, SEED);
  }
  return 0;
}

static int uds_listen(int sock, struct sockaddr_un *saddr) {
  int len;
  unlink(saddr->sun_path);

  len = offsetof(struct sockaddr_un, sun_path) + strlen(saddr->sun_path);
  if (bind(sock, (struct sockaddr *)(saddr), len) < 0) {
    fprintf(stderr, "error (%s, line %d): bind buf socket error\n", __FILE__,
            __LINE__);
    goto out;
  }

  if (listen(sock, 10) < 0) {
    fprintf(stderr, "error (%s, line %d): listen buf socket error\n", __FILE__,
            __LINE__);
    goto out;
  }

  return 0;

out:
  close(sock);
  return -1;
}

int RDMAServer::__init_buf_sock() {
  int sock, epoll_fd;
  struct sockaddr_un saddr;
  unsigned long len;

  // first prepare address
  memset(&saddr, 0, sizeof(struct sockaddr_un));
  saddr.sun_family = AF_UNIX;
  len = snprintf(saddr.sun_path, sizeof(saddr.sun_path), "%s", BUF_SOCK_ADDR);
  if (len >= sizeof(saddr.sun_path)) {
    fprintf(stderr, "Wrong UNIX socket name: %s\n", BUF_SOCK_ADDR);
    return -1;
  }

  if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
    fprintf(stderr, "error: create buf socket failed\n");
    return -1;
  }
  if (uds_listen(sock, &saddr))
    return -1;
  this->buf_sock_fd = sock;

  // setup epoll fd
  if (REPORT_IF_TRUE((epoll_fd = epoll_create(20)) < 0))
    return -1;
  this->buf_sock_epoll_fd = epoll_fd;
  return 0;
}

int RDMAServer::__serve_buf_sock() {
  int client_fd;
  struct sockaddr_in saddr;
  socklen_t sock_len;
  struct epoll_event event;
  // swapn a thread to poll epoll fd
  std::thread t(&RDMAServer::__epoll_buf_sock, this);
  t.detach();

  printf("buf sock ready to accept connections...\n");

  while (true) {
    client_fd = accept(buf_sock_fd, (struct sockaddr *)&saddr, &sock_len);
    if (REPORT_IF_TRUE(client_fd < 0))
      return -1;

    printf("new buf sock client connected: %d\n", client_fd);
    // add to epoll fd
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = client_fd;
    if (REPORT_IF_NONZERO(
            epoll_ctl(buf_sock_epoll_fd, EPOLL_CTL_ADD, client_fd, &event)))
      return -1;
  }
  return 0;
}

int RDMAServer::__epoll_buf_sock() {
  struct epoll_event ready_events[10];
  int nr_events, client_fd, cmd, ret, img_fd, ack = 0;
  unsigned long pgoff;
  while (true) {
    nr_events = epoll_wait(buf_sock_epoll_fd, ready_events, 10, -1);
    if (REPORT_IF_TRUE(nr_events < 0))
      return -1;
    for (int i = 0; i < nr_events; i++) {
      client_fd = ready_events[i].data.fd;
      // first recv command
      // then recv image
      // finally sync with criu
      if (ready_events[i].events & EPOLLIN) {
        ret = recv(client_fd, &cmd, sizeof(cmd), 0);
        if (ret == 0)
          goto check_next;
        if (REPORT_IF_TRUE(ret != sizeof(cmd)))
          return -1;
        switch (cmd) {
        case BUF_SOCK_MAP:
          if (REPORT_IF_NONZERO(
                  recv_fds(client_fd, &img_fd, 1, &pgoff, sizeof(pgoff))))
            return -1;
          if (REPORT_IF_NONZERO(__copy_img_to_buffer_poll(img_fd, pgoff)))
            return -1;
          // the img_fd has been consumed, remember to close it
          close(img_fd);
          img_fd = -1; // prevent it from been used further
          ret = send(client_fd, &ack, sizeof(ack), 0);
          if (ret != sizeof(ack)) {
            fprintf(stderr, "ack to criu failed\n");
            return -1;
          }
          break;
        default:
          fprintf(stderr, "recv unsupport command %d\n", cmd);
          return -1;
        }
      }
    check_next:
      if (ready_events[i].events & EPOLLERR) {
        printf("close client buf_sock %d due to err\n", client_fd);
        if (REPORT_IF_NONZERO(
                epoll_ctl(buf_sock_epoll_fd, EPOLL_CTL_DEL, client_fd, NULL)))
          return -1;
        close(client_fd);
      }
      if (ready_events[i].events & (EPOLLHUP | EPOLLRDHUP)) {
        printf("close client buf_sock %d due to hup\n", client_fd);
        if (REPORT_IF_NONZERO(
                epoll_ctl(buf_sock_epoll_fd, EPOLL_CTL_DEL, client_fd, NULL)))
          return -1;
        close(client_fd);
      }
    }
  }
  return 0;
}

static int get_path_of_fd(int fd, char *buf, int buf_size) {
  char path[1024];
  pid_t pid = getpid();
  int ret;
  sprintf(path, "/proc/%d/fd/%d", pid, fd);
  ret = readlink(path, buf, buf_size);
  if (ret < 0)
    return -errno;
  return 0;
}

// only one thread (i.e., epoll thread) will execute this
// no need to add locks for buffer
int RDMAServer::__copy_img_to_buffer_poll(int img_fd, unsigned long pgoff) {
  struct stat stat_buf;
  unsigned long total, nr_read = 0;
  char filepath_buf[1024];
  if (img_fd < 0) {
    fprintf(stderr, "get invalid file descriptor: %d\n", img_fd);
    return -1;
  }
  if (REPORT_IF_NONZERO(fstat(img_fd, &stat_buf)))
    return -1;
  total = stat_buf.st_size;
  if (total & (PAGE_SIZE - 1))
    printf("WARNING: get image size not aligned to page size: %ld\n", total);
  if (BUFFER_SIZE < (pgoff * PAGE_SIZE + total)) {
    fprintf(
        stderr,
        "buffer not enough: current size = %ld, pgoff = %ld, img_size = %ld\n",
        BUFFER_SIZE, pgoff, total);
    return -1;
  }

  if (REPORT_IF_NONZERO(get_path_of_fd(img_fd, filepath_buf, 1024)))
    return -1;

  // make sure we are copying from beginning
  //
  // Linux manual lseek(2):
  // Upon successful completion, lseek() returns the resulting offset
  // location as measured in bytes from the beginning of the file.  On
  // error, the value (off_t) -1 is returned and errno is set to
  // indicate the error.
  if (REPORT_IF_NONZERO(lseek(img_fd, 0, SEEK_SET)))
    return -1;
  printf("begin to copy image %s (size %ld bytes) to pgoff %ld\n", filepath_buf,
         total, pgoff);
  uint8_t *start = buffer + (pgoff * PAGE_SIZE);
  while (true) {
    int n = read(img_fd, start, total - nr_read);
    if (n == 0)
      break;
    if (n < 0) {
      fprintf(stderr, "read from image failed: %d\n", errno);
      return -1;
    }
    nr_read += n;
    start += n;
  }
  if (nr_read != total) {
    fprintf(stderr,
            "copy image size not enough: %ld(expected) vs %ld(actual)\n", total,
            nr_read);
    return -1;
  }
  return 0;
}
