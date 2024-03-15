#include "pseudo-mm-rdma-server.h"
#include <string.h>
#include <sys/sysinfo.h>

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "please specify port number: %s <port_number>", argv[0]);
    exit(EXIT_FAILURE);
  }
  uint16_t port = atoi(argv[1]);
  const int NUM_CPUS = get_nprocs();
  RDMAServer server(NUM_CPUS);
  if (REPORT_IF_NONZERO(server.init(port)))
    exit(EXIT_FAILURE);
	printf("server init finish\n");
  if (REPORT_IF_NONZERO(server.connect_queue()))
    exit(EXIT_FAILURE);
	printf("server connect queue finish, start serve()...\n");
  if (REPORT_IF_NONZERO(server.serve()))
    exit(EXIT_FAILURE);
  return 0;
}
