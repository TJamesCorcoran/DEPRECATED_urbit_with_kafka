/* v/egzh.c
**
** This file is in the public domain.
*/

// there are two methods of storing events:
//    * egz.hope log file
//    * kafka log servers
// this code implementes function around the former.

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>
#include <dirent.h>

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pwd.h>

#include "all.h"
#include "v/vere.h"
#include "v/sist.h"
#include "v/egzh.h"

#if defined(U2_OS_linux)
#include <stdio_ext.h>
#define fpurge(fd) __fpurge(fd)
#define DEVRANDOM "/dev/urandom"
#else
#define DEVRANDOM "/dev/random"
#endif



  /* _sist_rest_nuu(): upgrade log from previous format.
   */
static void
_egz_rest_nuu(u2_ulog* lug_u, u2_uled led_u, c3_c* old_c)
{
  c3_c    nuu_c[2048];
  u2_noun roe = u2_nul;
  c3_i    fid_i = lug_u->fid_i;
  c3_i    fud_i;
  c3_i    ret_i;
  c3_d    end_d = lug_u->len_d;

  uL(fprintf(uH, "egzh: converting log from prior format\n"));

  c3_assert(led_u.mag_l == u2_mug('f'));

  if ( -1 == lseek64(fid_i, 4ULL * end_d, SEEK_SET) ) {
    uL(fprintf(uH, "rest_nuu failed (a)\n"));
    perror("lseek64");
    u2_lo_bail(u2A);
  }

  while ( end_d != c3_wiseof(u2_uled) ) {
    c3_d    tar_d;
    u2_olar lar_u;
    c3_w*   img_w;
    u2_noun ron;

    tar_d = (end_d - (c3_d)c3_wiseof(u2_olar));

    if ( -1 == lseek64(fid_i, 4ULL * tar_d, SEEK_SET) ) {
      uL(fprintf(uH, "rest_nuu failed (b)\n"));
      perror("lseek64");
      u2_lo_bail(u2A);
    }
    if ( sizeof(u2_olar) != read(fid_i, &lar_u, sizeof(u2_olar)) ) {
      uL(fprintf(uH, "rest_nuu failed (c)\n"));
      perror("read");
      u2_lo_bail(u2A);
    }

    if ( lar_u.syn_w != u2_mug((c3_w)tar_d) ) {
      uL(fprintf(uH, "rest_nuu failed (d)\n"));
      u2_lo_bail(u2A);
    }

    img_w = c3_malloc(4 * lar_u.len_w);
    end_d = (tar_d - (c3_d)lar_u.len_w);

    if ( -1 == lseek64(fid_i, 4ULL * end_d, SEEK_SET) ) {
      uL(fprintf(uH, "rest_nuu failed (e)\n"));
      perror("lseek64");
      u2_lo_bail(u2A);
    }
    if ( (4 * lar_u.len_w) != read(fid_i, img_w, (4 * lar_u.len_w)) ) {
      uL(fprintf(uH, "rest_nuu failed (f)\n"));
      perror("read");
      u2_lo_bail(u2A);
    }

    ron = u2_ci_words(lar_u.len_w, img_w);
    free(img_w);

    if ( lar_u.mug_w != u2_cr_mug(ron) ) {
      uL(fprintf(uH, "rest_nuu failed (g)\n"));
      u2_lo_bail(u2A);
    }

    roe = u2nc(ron, roe);
  }

  if ( 0 != close(fid_i) ) {
    uL(fprintf(uH, "egzh: could not close\n"));
    perror("close");
    u2_lo_bail(u2A);
  }

  c3_c bas_c[2048];
  u2_sist_get_pier_dirstr(bas_c, 2048);
  ret_i = snprintf(nuu_c, 2048, "%s/.urb/ham.hope", bas_c);
  c3_assert(ret_i < 2048);

  if ( (fud_i = open(nuu_c, O_CREAT | O_TRUNC | O_RDWR, 0600)) < 0 ) {
    uL(fprintf(uH, "egzh: can't open record (%s)\n", nuu_c));
    perror("open");
    u2_lo_bail(u2A);
  }

  led_u.mag_l = u2_mug('g');
  if ( (sizeof(led_u) != write(fud_i, &led_u, sizeof(led_u))) ) {
    uL(fprintf(uH, "egzh: can't write header\n"));
    perror("write");
    u2_lo_bail(u2A);
  }

  {
    c3_d ent_d = 1;

    c3_assert(end_d == c3_wiseof(u2_uled));
    while ( u2_nul != roe ) {
      u2_noun ovo = u2k(u2h(roe));
      u2_noun nex = u2k(u2t(roe));
      u2_ular lar_u;
      c3_w*   img_w;
      c3_d    tar_d;

      lar_u.len_w = u2_cr_met(5, ovo);
      tar_d = end_d + lar_u.len_w;
      lar_u.syn_w = u2_cr_mug(tar_d);
      lar_u.ent_d = ent_d;
      lar_u.tem_w = 0;
      lar_u.typ_w = c3__ov;
      lar_u.mug_w = u2_cr_mug_both(u2_cr_mug(ovo),
                                   u2_cr_mug_both(u2_cr_mug(0),
                                                  u2_cr_mug(c3__ov)));

      img_w = c3_malloc(lar_u.len_w << 2);
      u2_cr_words(0, lar_u.len_w, img_w, ovo);
      u2z(ovo);

      if ( (lar_u.len_w << 2) != write(fud_i, img_w, lar_u.len_w << 2) ) {
        uL(fprintf(uH, "rest_nuu failed (h)\n"));
        perror("write");
        u2_lo_bail(u2A);
      }
      if ( sizeof(u2_ular) != write(fud_i, &lar_u, sizeof(u2_ular)) ) {
        uL(fprintf(uH, "rest_nuu failed (i)\n"));
        perror("write");
        u2_lo_bail(u2A);
      }

      ent_d++;
      end_d = tar_d + c3_wiseof(u2_ular);
      u2z(roe); roe = nex;
    }
  }
  if ( 0 != rename(nuu_c, old_c) ) {
    uL(fprintf(uH, "rest_nuu failed (k)\n"));
    perror("rename");
    u2_lo_bail(u2A);
  }
  if ( -1 == lseek64(fud_i, sizeof(u2_uled), SEEK_SET) ) {
    uL(fprintf(uH, "rest_nuu failed (l)\n"));
    perror("lseek64");
    u2_lo_bail(u2A);
  }
  lug_u->fid_i = fud_i;
  lug_u->len_d = end_d;
}


// open the egz.hope file
//   * sanity check:
//        * egz exists
//        * is not corrupt,
//        * arvo kernel versions match
//        * etc
//   * store details
//        * egz file handle in u2R->lug_u
//        * tno_l (msg number) in 
c3_t u2_egz_open(u2_reck* rec_u, c3_i * fid_i, u2_uled * led_u)
{
  struct stat buf_b;
  u2_noun     sev_l; // , sal_l, key_l, tno_l;

  c3_i pig_i = O_RDWR;
#ifdef O_DSYNC
  pig_i |= O_DSYNC;
#endif

  c3_c        ful_c[2048];
  u2_sist_get_egz_filestr(ful_c, 2048);

  if ( ((*fid_i = open(ful_c, pig_i)) < 0) || (fstat(*fid_i, &buf_b) < 0) ) {
    uL(fprintf(uH, "egzh: can't open record (%s)\n", ful_c));
    u2_lo_bail(rec_u);

    return c3_false;
  }
#ifdef F_NOCACHE
  if ( -1 == fcntl(*fid_i, F_NOCACHE, 1) ) {
    uL(fprintf(uH, "egzh: can't uncache %s: %s\n", ful_c, strerror(errno)));
    u2_lo_bail(rec_u);

    return c3_false;
  }
#endif
  u2R->lug_u.fid_i = *fid_i;
  u2R->lug_u.len_d = ((buf_b.st_size + 3ULL) >> 2ULL);

  c3_w        size_w;

  size_w = read(*fid_i, led_u, sizeof(*led_u));
  if ( sizeof(*led_u) != size_w)  {
    fprintf(stderr, "egzh: record is corrupt (a)\n");
    u2_lo_bail(rec_u);
  }

  if ( u2_mug('f') == led_u->mag_l ) {
    _egz_rest_nuu(&u2R->lug_u, *led_u, ful_c);
    *fid_i = u2R->lug_u.fid_i;
  }
  else if (u2_mug('g') != led_u->mag_l ) {
    fprintf(stderr, "egzh: record is obsolete (or corrupt)\n");
    u2_lo_bail(rec_u);
  }

  if ( led_u->kno_w != rec_u->kno_w ) {
    //  XX perhaps we should actually do something here
    //
    fprintf(stderr, "egzh: (not) translating events (old %d, now %d)\n",
               led_u->kno_w,
               rec_u->kno_w);
  }

  sev_l = led_u->sev_l;
  /* sal_l = led_u->sal_l; */
  /* key_l = led_u->key_l; */
  /* tno_l = led_u->tno_l; */

  {
    u2_noun old = u2_dc("scot", c3__uv, sev_l);
    u2_noun nuu = u2_dc("scot", c3__uv, rec_u->sev_l);
    c3_c* old_c = u2_cr_string(old);
    c3_c* nuu_c = u2_cr_string(nuu);

    uL(fprintf(uH, "egzh: old %s, new %s\n", old_c, nuu_c));
    free(old_c); free(nuu_c);

    u2z(old); u2z(nuu);
  }
  c3_assert(sev_l != rec_u->sev_l);   //  1 in 2 billion, just retry


  fprintf(stderr, "egzh: opened without error\n");

  //  check passcode
  // NOTFORCHECKIN  _sist_passcode(rec_u, sal_l);

  return c3_true;
}

// read from the file. Return one noun which is event number of first item.
//
// input args:
//    * rec_u
//    * fid_i - file handle of already open egz.hope
// output args:
//    * ohh   - ???
//
// return:
//    * first read noun, which is a cell structure, which means that later we can iterate over it by using
//      u2h() and u2t().  All reading from  disk should be done in this func!

u2_noun u2_egz_read_all(u2_reck* rec_u, c3_i fid_i,   u2_bean *  ohh)
{
  c3_d    ent_d;
  c3_d    end_d;
  c3_d    las_d = 0;
  c3_d    old_d = rec_u->ent_d;
  u2_bean rup = u2_no;
  u2_noun roe = u2_nul;

  end_d = u2R->lug_u.len_d;
  ent_d = 0;

  if ( -1 == lseek64(fid_i, 4ULL * end_d, SEEK_SET) ) {
    fprintf(stderr, "end_d %llu\n", end_d);
    perror("lseek");
    uL(fprintf(uH, "record is corrupt (c)\n"));
    u2_lo_bail(rec_u);
  }

  while ( end_d != c3_wiseof(u2_uled) ) {
    c3_d    tar_d = (end_d - (c3_d)c3_wiseof(u2_ular));
    u2_ular lar_u;
    c3_w*   img_w;
    u2_noun ron;

    // uL(fprintf(uH, "egzh: reading event at %llx\n", end_d));

    if ( -1 == lseek64(fid_i, 4ULL * tar_d, SEEK_SET) ) {
      uL(fprintf(uH, "record is corrupt (d)\n"));
      u2_lo_bail(rec_u);
    }
    if ( sizeof(u2_ular) != read(fid_i, &lar_u, sizeof(u2_ular)) ) {
      uL(fprintf(uH, "record is corrupt (e)\n"));
      u2_lo_bail(rec_u);
    }

    if ( lar_u.syn_w != u2_mug((c3_w)tar_d) ) {
      if ( u2_no == rup ) {
        uL(fprintf(uH, "corruption detected; attempting to fix\n"));
        rup = u2_yes;
      }
      uL(fprintf(uH, "lar:%x mug:%x\n", lar_u.syn_w, u2_mug((c3_w)tar_d)));
      end_d--; u2R->lug_u.len_d--;
      continue;
    }
    else if ( u2_yes == rup ) {
      uL(fprintf(uH, "matched at %x\n", lar_u.syn_w));
      rup = u2_no;
    }

    if ( lar_u.ent_d == 0 ) {
      *ohh = u2_yes;
    }

#if 0
    //        uL(fprintf(uH, "log: read: at %d, %d: lar ent %llu, len %d, mug %x\n",
    //                   (tar_w - lar_u.len_w),
    //                   tar_w,
    //                   lar_u.ent_d,
    //                   lar_u.len_w,
    //                   lar_u.mug_w));
#endif
    if ( end_d == u2R->lug_u.len_d ) {
      ent_d = las_d = lar_u.ent_d;
    }
    else {
      if ( lar_u.ent_d != (ent_d - 1ULL) ) {
        uL(fprintf(uH, "record is corrupt (g)\n"));
        uL(fprintf(uH, "lar_u.ent_d %llx, ent_d %llx\n", lar_u.ent_d, ent_d));
        u2_lo_bail(rec_u);
      }
      ent_d -= 1ULL;
    }
    end_d = (tar_d - (c3_d)lar_u.len_w);

    if ( ent_d < old_d ) {
      //  XX this could be a break if we didn't want to see the sequence
      //  number of the first event.
      continue;
    }

    img_w = c3_malloc(4 * lar_u.len_w);

    if ( -1 == lseek64(fid_i, 4ULL * end_d, SEEK_SET) ) {
      uL(fprintf(uH, "record is corrupt (h)\n"));
      u2_lo_bail(rec_u);
    }
    if ( (4 * lar_u.len_w) != read(fid_i, img_w, (4 * lar_u.len_w)) ) {
      uL(fprintf(uH, "record is corrupt (i)\n"));
      u2_lo_bail(rec_u);
    }

    // construct a noun from  the raw data
    //   1) process raw data (???)
    //   2) check hashing
    //   3) decrypt
    ron = u2_ci_words(lar_u.len_w, img_w);
    free(img_w);

    if ( lar_u.mug_w !=
         u2_cr_mug_both(u2_cr_mug(ron),
                        u2_cr_mug_both(u2_cr_mug(lar_u.tem_w),
                                       u2_cr_mug(lar_u.typ_w))) )
      {
        uL(fprintf(uH, "record is corrupt (j)\n"));
        u2_lo_bail(rec_u);
      }

    if ( c3__ov != lar_u.typ_w ) {
      u2z(ron);
      continue;
    }

    if ( rec_u->key ) {
      u2_noun dep;

      dep = u2_dc("de:crua", u2k(rec_u->key), ron);
      if ( u2_no == u2du(dep) ) {
        uL(fprintf(uH, "record is corrupt (k)\n"));
        u2_lo_bail(rec_u);
      }
      else {
        ron = u2k(u2t(dep));
        u2z(dep);
      }
    }
    roe = u2nc(u2_cke_cue(ron), roe);
  }
  rec_u->ent_d = c3_max(las_d + 1ULL, old_d);

  exit(-1); // NOTFORCHECKIN - unimplemented!
  u2_noun uglyhack_u = (u2_noun) malloc(sizeof(u2_noun));
  return( uglyhack_u);
}

// Create a new egz file w default header
//
void u2_egz_write_header(u2_reck* rec_u, c3_l sal_l)
{
  c3_i pig_i = O_CREAT | O_WRONLY | O_EXCL;
#ifdef O_DSYNC
  pig_i |= O_DSYNC;
#endif

  c3_c    ful_c[2048];  
  u2_sist_get_egz_filestr(ful_c, 2048);
  struct stat buf_b;
  c3_i        fid_i;

  if ( ((fid_i = open(ful_c, pig_i, 0600)) < 0) ||
       (fstat(fid_i, &buf_b) < 0) )
    {
      uL(fprintf(uH, "can't create record (%s)\n", ful_c));
      u2_lo_bail(rec_u);
    }
#ifdef F_NOCACHE
  if ( -1 == fcntl(fid_i, F_NOCACHE, 1) ) {
    uL(fprintf(uH, "zest: can't uncache %s: %s\n", ful_c, strerror(errno)));
    u2_lo_bail(rec_u);
  }
#endif

  u2R->lug_u.fid_i = fid_i;
  
  u2_uled led_u;

  led_u.mag_l = u2_mug('g');
  led_u.kno_w = rec_u->kno_w;

  if ( 0 == rec_u->key ) {
    led_u.key_l = 0;
  } else {
    led_u.key_l = u2_mug(rec_u->key);

    c3_assert(!(led_u.key_l >> 31));
  }
  led_u.sal_l = sal_l;
  led_u.sev_l = rec_u->sev_l;
  led_u.tno_l = 1;  // egg count init to 1

  c3_w        size_w =  write(fid_i, &led_u, sizeof(led_u)); 

  if ( sizeof(led_u) != size_w ) {
    uL(fprintf(uH, "can't write record (%s)\n", ful_c));
    u2_lo_bail(rec_u);
  }

  u2R->lug_u.len_d = c3_wiseof(led_u);

  syncfs(fid_i); // NOTFORCHECKIN

}



void u2_egz_rewrite_header(u2_reck* rec_u, c3_i fid_i, u2_bean ohh, u2_uled * led_u)
{
  //  Increment sequence numbers. New logs start at 1.
  if ( u2_yes == ohh ) {
    uL(fprintf(uH, "egzh: bumping ent_d, don't panic.\n"));
    u2_ular lar_u;
    c3_d    end_d;
    c3_d    tar_d;

    rec_u->ent_d++;
    end_d = u2R->lug_u.len_d;
    while ( end_d != c3_wiseof(u2_uled) ) {
      tar_d = end_d - c3_wiseof(u2_ular);
      if ( -1 == lseek64(fid_i, 4ULL * tar_d, SEEK_SET) ) {
        uL(fprintf(uH, "bumping sequence numbers failed (a)\n"));
        u2_lo_bail(rec_u);
      }
      if ( sizeof(lar_u) != read(fid_i, &lar_u, sizeof(lar_u)) ) {
        uL(fprintf(uH, "bumping sequence numbers failed (b)\n"));
        u2_lo_bail(rec_u);
      }
      lar_u.ent_d++;
      if ( -1 == lseek64(fid_i, 4ULL * tar_d, SEEK_SET) ) {
        uL(fprintf(uH, "bumping sequence numbers failed (c)\n"));
        u2_lo_bail(rec_u);
      }
      if ( sizeof(lar_u) != write(fid_i, &lar_u, sizeof(lar_u)) ) {
        uL(fprintf(uH, "bumping sequence numbers failed (d)\n"));
        u2_lo_bail(rec_u);
      }
      end_d = tar_d - lar_u.len_w;
    }
  }

  led_u->mag_l = u2_mug('g');
  // led_u->sal_l stays the same
  led_u->sev_l = rec_u->sev_l;
  led_u->key_l = rec_u->key ? u2_mug(rec_u->key) : 0;
  led_u->kno_w = rec_u->kno_w;         //  may need actual translation!
  led_u->tno_l = 1;                    // why sequence # 1 ?

  if ( (-1 == lseek64(fid_i, 0, SEEK_SET)) ||
       (sizeof(led_u) != write(fid_i, led_u, sizeof(u2_uled))) )
    {
      uL(fprintf(uH, "record failed to rewrite\n"));
      u2_lo_bail(rec_u);
    }
}

// Quickly log an ovum via egz mini files.
// Note that we will consolidate these mini files later in the consolidator thread.
// 
// inputs:
//   * raf_u - pointer to raft state structure (pass in global 'u2A')
//   * bob_w - msg
//   * len_w - msg len
c3_d
u2_egz_push(u2_raft* raf_u, c3_w* bob_w, c3_w len_w)
{
  c3_assert(0 != bob_w && 0 < len_w);
  
  // NOTFORCHECKIN not writing u2_uled header or u2_ular trailer; do that in 
  // consolidator, if we care to

  // get sequence number
  c3_d seq_d = u2A->ent_d++;

  // get file name
  c3_c bas_c[2048];
  u2_sist_get_egz_quick_filestr(bas_c, 2048, seq_d);

  // open, write, close file
  c3_i fid_i = open(bas_c, O_CREAT | O_TRUNC | O_WRONLY, 0600);
  if (fid_i < 0){
    fprintf(stderr, "egz_push() failed - fopen - %s\n", strerror(errno));
    exit(1);
  }

  c3_w actual_len_w = write(fid_i, bob_w, len_w);
  if (actual_len_w == -1 ){
    fprintf(stderr, "egz_push() failed - write - %s\n", strerror(errno));
    exit(1);
  }
  if (0 != close(fid_i)){
    uL(fprintf(uH, "egz_push() failed - close\n"));
    exit(1);
  }

  // N.B. that we do not do an expensive fsync() here.  Sync will
  // happen on its own, but we're not going to slow down this primary work thread to
  // wait for it.  The consolidator (in its own thread) will pick up
  // the task by noting the file on disk later.

  return(u2A->ent_d);
}

// copy-and-paste programming; see also u2_kafka_push_ova()
//
// input:
//    * rec_u
//    * ovo
// return:
//    * id of log msg
c3_d
u2_egz_push_ova(u2_reck* rec_u, u2_noun ovo)
{
  u2_noun ron;
  c3_d    bid_d;

  // serialize
  ron = u2_cke_jam(u2nc(u2k(rec_u->now), ovo));
  c3_assert(rec_u->key);

  // encrypt
  ron = u2_dc("en:crua", u2k(rec_u->key), ron);

  // copy data to raft_bob_w, manage ref counts
  c3_w    len_w;
  c3_w*   bob_w;

  len_w = u2_cr_met(5, ron);
  bob_w = c3_malloc(len_w * 4L);
  u2_cr_words(0, len_w, bob_w, ron);
  bid_d = u2_egz_push(u2R, bob_w, len_w);

  u2z(ron);  

  return(bid_d);
}



void _egz_consolidator(void *arg)
{
  fprintf(stdout, "egzh: consolidator live\n");

  while(1) {
    sleep(10);

    DIR *dir;
    struct dirent *ent;
    c3_c    egzdir_c[2048];  
    u2_sist_get_egz_quick_dirstr(egzdir_c, 2048);

    if ((dir = opendir (egzdir_c))) {
        while ((ent = readdir (dir)) != NULL) {
          if (strcmp(".", ent->d_name)) continue;
          if (strcmp("..", ent->d_name)) continue;
          fprintf(stdout, "egzh: file found: %s\n", ent->d_name);

          // NOTFORCHECKIN - do something here!

        }
        closedir (dir);

      } else {
        fprintf(stderr, "egzh: error opening egz_quickdir: %s\n", strerror(errno));
      }
      
      
  }

}


void u2_egz_init()
{
  fprintf(stdout, "egzh: egz.hope logging begin\n");

  // note that this new thread is not the libuv event loop and has nothing to do w the libuv event loop
  uv_thread_create(& u2K->egz_consolidator_thread_u,
                   & _egz_consolidator,
                   NULL);
  fprintf(stdout, "egzh: primary thread\n");
}

u2_reck *  u2_egz_util_get_u2a()
{
  return(u2A);
}

u2_kafk *  u2_egz_util_get_u2k()
{
  return(u2K);
}
