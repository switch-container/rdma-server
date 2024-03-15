#ifndef _PSEUDO_MM_RDMA_SERVER_H_
#define _PSEUDO_MM_RDMA_SERVER_H_

#include <cstdint>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define REPORT_IF_ZERO(x)                                                      \
  ({                                                                           \
    bool ____ret = !(x);                                                       \
    do {                                                                       \
      if (____ret)                                                             \
        fprintf(stderr, "error: " #x "failed (return zero/null).\n");          \
      fflush(stderr);                                                          \
    } while (0);                                                               \
    ____ret;                                                                   \
  })

#define REPORT_IF_NONZERO(x)                                                   \
  ({                                                                           \
    bool ____ret = (x);                                                        \
    do {                                                                       \
      if (____ret)                                                             \
        fprintf(stderr, "error: " #x "failed (return non-zero).\n");           \
      fflush(stderr);                                                          \
    } while (0);                                                               \
    ____ret;                                                                   \
  })

#define PAGE_SIZE (1 << 12)
#define GB (1UL << 30)
#define BUFFER_SIZE (1 * GB)

const unsigned long SEED = 0xabcabc;

struct Device {
  struct ibv_pd *pd;
  struct ibv_context *verbs;
};

class RDMAQueue {
  friend class RDMAServer;

public:
  RDMAQueue()
      : qp(nullptr), cq(nullptr), client_cm_id(nullptr), srv(nullptr),
        state(INIT) {}
  int create_qp();
  struct ibv_qp *qp;
  struct ibv_cq *cq;
  struct rdma_cm_id *client_cm_id; // this is client cm id
  struct RDMAServer *srv;
  enum { INIT, CONNECTED } state;
};

extern "C" {
struct Memregion {
  uint64_t baseaddr;
  uint32_t key;
};
}

class RDMAServer {
  friend class RDMAQueue;

public:
  RDMAServer(int queue_num);
  ~RDMAServer();
  // return 0 when succeed
  int init(uint16_t port_num);
  // return 0 when succeed
  int connect_queue();
  int serve();
  int fill_buffer_poll();

protected:
  int __init_device(struct rdma_cm_id *client_cm_id);
  int __register_buffer_pool();
  int __handle_one_event();
  int __handle_connect_request(struct rdma_cm_id *client_cm_id,
                               struct rdma_conn_param *param);
  int __handle_establish(struct rdma_cm_id *client_cm_id);
  int __handle_disconnect(struct rdma_cm_id *client_cm_id);
  int __poll_for_mr_send();
  int __get_connected_queue_num() const;

  RDMAQueue *queues = nullptr;
  int queue_num;
  uint8_t *buffer = nullptr; // memory buffer
  Device *dev = nullptr;

  int connected_queue = 0;

  struct ibv_mr *memory_region = nullptr;
  struct ibv_comp_channel *comp_channel = nullptr;
  struct rdma_event_channel *ec = nullptr;
  struct rdma_cm_id *srv_cm_id = nullptr;
  struct ibv_cq *cq = nullptr;
};

#endif
