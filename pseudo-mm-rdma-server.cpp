// This is originated by fastswap rmserver !
#include <cassert>
#include <rdma/rdma_cma.h>
#include <string.h>
#include <sys/sysinfo.h>

#include "pseudo-mm-rdma-server.h"

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
  if (REPORT_IF_NONZERO(fill_buffer_poll()))
    return 1;
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
