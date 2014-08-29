#include "test_main.h"

// these tests will be run inside a full running vere 
//
void run_tests()
{
  // WORKS test_clog_unclog_setup();

  // WORKS test_egz_queue_setup();
  // WORKS test_egz_bytes_setup();
  // WORKS test_egz_ova_setup();

  // test_egz_stress();

  // WORKS test_kafka_logging_bytes();
  // test_kafka_logging_ova_setup();

  test_kafk_stress();
  // test_kafka_logging_ova_setup();

  exit(1);
}

