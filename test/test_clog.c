#include "test_main.h"

void _test_clog_unclog()
{
  bool verbose = false;
  u2_Host.arv_u->key = 1;
  _sist_home(u2A);


  char * payload_str = "hello nouns";
  if (verbose) { printf("input was: %s\n", payload_str); }
  u2_noun aaa = u2_ci_string(payload_str);  

  c3_w malloc_w;
  c3_w len_w;
  c3_y * complete_y;
  u2_clog_o2b(aaa, &malloc_w, & len_w, & complete_y);

  //  c3_y * header_y = complete_y;
  c3_y * data_y   = complete_y + sizeof(u2_clpr);
  
  u2_noun bbb;
  u2_clog_b2o(len_w, data_y, & bbb);

  c3_c* output_c = u2_cr_string(bbb);

  if (verbose) { printf("output is: %s\n", output_c); }

  if (0 == strcmp(payload_str, output_c)){
    printf("PASS - test_clog_unclog\n");
  } else {
    printf("FAIL - test_clog_unclog\n");
    exit(-1);
  }

  // release storage
  free(complete_y);

}

void test_clog_unclog_setup()
{
  setup_loop();
  util_run_inside_loop( & _test_clog_unclog, NULL );
  util_run_after_timer( & util_end_test, NULL, 2 * 1000, 0);
  util_run_loop();
}
