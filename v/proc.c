// v/proc.c  (formerly v/raft.c)
//
// This file is in the public domain.
//
//
// This code processes atoms.
//   * inject a new atom: u2_proc_plan() / u2_proc_plow()
//   * process atoms:     u2_proc_work()
//
// It is coupled closely with the logging tools egzh and kafk,
// 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

#include "all.h"
#include "v/vere.h"
#include "v/sist.h"
#include "v/egzh.h"
#include "v/kafk.h"

static void _proc_sure_guard(u2_reck* rec_u, u2_noun ovo, u2_noun vir, u2_noun cor, c3_t force_delay);
static void _proc_sure(u2_reck* rec_u, u2_noun ovo, u2_noun vir, u2_noun cor);

//  This is the final step of some tricky coordination.
//
//  We must (a) log each ova, (b) process each ova.  We may do those things in parallel, but 
//  both must be done before we can update the global state and emit side effects.
//
//  _proc_sure is that final step.  
//
//  This function guards _proc_sure.  Either you are ready to call proc_sure right now, 
//
//  force_delay
static void 
_proc_sure_guard(u2_reck* rec_u, u2_noun ovo, u2_noun vir, u2_noun cor, c3_t force_delay)
{
  //  if ( ( rec_u->ova.egg_u->log) && (rec_u->ova.egg_u->done) && force_delay  ) {
    _proc_sure(rec_u, ovo, vir, cor);
    //  } else {
    // NOTFORCHECKIN - here we have to enqueue a task for later
    //  }
}

/* _proc_sure(): apply and save an input ovum and its result.
*/
static void
_proc_sure(u2_reck* rec_u, u2_noun ovo, u2_noun vir, u2_noun cor)
{
  //  Whatever worked, save it.  (XX - should be concurrent with execute.)
  //  We'd like more events that don't change the state but need work here.
  {
    u2_mug(cor);
    u2_mug(rec_u->roc);

    // we've got a new candidate world state.  Perhaps it's identical
    // to where we are now. Perhaps its new.
    //
    // * if new: update world state
    // * in either case: push side effects event into queue
    //
    if ( u2_no == u2_sing(cor, rec_u->roc) ) {
      rec_u->roe = u2nc(u2nc(vir, ovo), rec_u->roe);

      u2z(rec_u->roc);

      // update universe
      rec_u->roc = cor;
    }
    else {
      u2z(ovo);

      // push a new event into queue
      rec_u->roe = u2nc(u2nc(vir, u2_nul), rec_u->roe);

      u2z(cor);
    }
  }
}

/* _proc_lame(): handle an application failure.
*/
static void
_proc_lame(u2_reck* rec_u, u2_noun ovo, u2_noun why, u2_noun tan)
{
  u2_noun bov, gon;

#if 0
  {
    c3_c* oik_c = u2_cr_string(u2h(u2t(ovo)));

    // uL(fprintf(uH, "lame: %s\n", oik_c));
    free(oik_c);
  }
#endif

  //  Formal error in a network packet generates a hole card.
  //
  //  There should be a separate path for crypto failures,
  //  to prevent timing attacks, but isn't right now.  To deal
  //  with a crypto failure, just drop the packet.
  //
  if ( (c3__exit == why) && (c3__hear == u2h(u2t(ovo))) ) {
    u2_lo_punt(2, u2_ckb_flop(u2k(tan)));

    bov = u2nc(u2k(u2h(ovo)), u2nc(c3__hole, u2k(u2t(u2t(ovo)))));
    u2z(why);
  }
  else {
    bov = u2nc(u2k(u2h(ovo)), u2nt(c3__crud, why, u2k(tan)));
    u2_hevn_at(lad) = u2_nul;
  }
  // u2_lo_show("data", u2k(u2t(u2t(ovo))));

  u2z(ovo);

  gon = u2_lo_soft(rec_u, 0, u2_reck_poke, u2k(bov));
  if ( u2_blip == u2h(gon) ) {
    _proc_sure_guard(rec_u, bov, u2k(u2h(u2t(gon))), u2k(u2t(u2t(gon))), c3_false);

    u2z(gon);
  }
  else {
    u2z(gon);
    {
      u2_noun vab = u2nc(u2k(u2h(bov)),
                         u2nc(c3__warn, u2_ci_tape("crude crash!")));
      u2_noun nog = u2_lo_soft(rec_u, 0, u2_reck_poke, u2k(vab));

      if ( u2_blip == u2h(nog) ) {
        _proc_sure_guard(rec_u, vab, u2k(u2h(u2t(nog))), u2k(u2t(u2t(nog))), c3_false);
        u2z(nog);
      }
      else {
        u2z(nog);
        u2z(vab);

        uL(fprintf(uH, "crude: all delivery failed!\n"));
        u2_lo_punt(2, u2_ckb_flop(u2k(tan)));
        c3_assert(!"crud");
      }
    }
  }
}


// _proc_punk(): insert and apply an input ovum (unprotected).
//
static void
_proc_punk(u2_reck* rec_u, u2_noun ovo)
{
#ifdef GHETTO
  c3_c* txt_c = u2_cr_string(u2h(u2t(ovo)));
#endif
  c3_w sec_w;
  //  static c3_w num_w;
  u2_noun gon;

  //  uL(fprintf(uH, "punk: %s: %d\n", u2_cr_string(u2h(u2t(ovo))), num_w++));

  //  XX this is wrong - the timer should be on the original hose.
  //
  if ( (c3__term == u2h(u2t(u2h(ovo)))) ||
       (c3__batz == u2h(u2t(u2h(ovo)))) ) {
    sec_w = 0;
  } else sec_w = 60;

  //  Control alarm loops.
  //
  if ( c3__wake != u2h(u2t(ovo)) ) {
    u2_Host.beh_u.run_w = 0;
  }

#ifdef GHETTO
  struct timeval b4, f2, d0;
  gettimeofday(&b4, 0);
  uL(fprintf(uH, "%%soft %s\n", txt_c));
#endif

  // TJIC: this is what calls into arvo proper
  // does NOT modify global arvo state.  gon is a list of ova which are
  // EFFECTS ** AND ** the new kernel state

  gon = u2_lo_soft(rec_u, sec_w, u2_reck_poke, u2k(ovo));  
#ifdef GHETTO
  c3_w ms_w;

  gettimeofday(&f2, 0);
  timersub(&f2, &b4, &d0);
  ms_w = (d0.tv_sec * 1000) + (d0.tv_usec / 1000);
  uL(fprintf(uH, "%%punk %s %d.%dms\n", txt_c, ms_w, (d0.tv_usec % 1000) / 10));
  free(txt_c);
#endif

  // error case
  if ( u2_blip != u2h(gon) ) {
    u2_noun why = u2k(u2h(gon));
    u2_noun tan = u2k(u2t(gon));

    u2z(gon);
    _proc_lame(rec_u, ovo, why, tan);
  }
  // TJIC success; operate on gon
  else {
    u2_noun vir = u2k(u2h(u2t(gon)));  // TJIC  vir = side effects  // c3_hear packets  "I want to hear that again"
    u2_noun cor = u2k(u2t(u2t(gon)));  // TJIC  cor = new rock (new value of system)
    u2_noun nug;

    u2z(gon);
    nug = u2_reck_nick(rec_u, vir, cor);  // <---- TJIC event transformations can happen here

    if ( u2_blip != u2h(nug) ) {
      u2_noun why = u2k(u2h(nug));
      u2_noun tan = u2k(u2t(nug));

      u2z(nug);
      _proc_lame(rec_u, ovo, why, tan);  // <---- TJIC event transformations can happen here
    }
    else {
      vir = u2k(u2h(u2t(nug)));
      cor = u2k(u2t(u2t(nug)));

      u2z(nug);
      _proc_sure_guard(rec_u, ovo, vir, cor, c3_false);
    }
  }
  //  uL(fprintf(uH, "punk oot %s\n", txt_c));
  //  free(txt_c);
}




/* _proc_kick_all(): kick a list of events, transferring.
*/
static void
_proc_kick_all(u2_reck* rec_u, u2_noun vir)
{
  while ( u2_nul != vir ) {
    u2_noun ovo = u2k(u2h(vir));
    u2_noun nex = u2k(u2t(vir));
    u2z(vir); vir = nex;

    u2_reck_kick(rec_u, ovo);
  }
}

static void
_raft_promote(u2_proc* raf_u)
{
  uL(fprintf(uH, "raft:      -> lead\n"));

  //  TODO boot in multiuser mode
  u2_sist_boot();
  if ( u2_no == u2_Host.ops_u.bat ) {
    u2_lo_lead(u2A);
  }

}

static void
_raft_lone_init(u2_proc* raf_u)
{
  uL(fprintf(uH, "raft: single-instance mode\n"));
  raf_u->pop_w = 1;
  _raft_promote(raf_u);
}


void
u2_proc_init()
{
  u2_proc* raf_u = u2R;

  //  Initialize timer -- used in both single and multi-instance mode,
  //  for different things.
  uv_timer_init(u2L, &raf_u->tim_u);
  raf_u->tim_u.data = raf_u;

  _raft_lone_init(raf_u);

}



// u2_reck_plan(): queue ovum (external).
//
void
u2_proc_plan(u2_reck* rec_u,
             u2_noun  pax,
             u2_noun  fav)
{
  //  if ( u2_raty_lead == u2R->typ_e ) {
    u2_noun egg = u2nc(pax, fav);
    rec_u->roe = u2nc(u2nc(u2_nul, egg), rec_u->roe);
    //  }
    //  else {
    //    c3_c* hed_c = u2_cr_string(u2h(u2t(pax)));
    ////    uL(fprintf(uH, "reck: dropping roe from %s\n", hed_c));
    ////    free(hed_c);
    ////    u2z(pax); u2z(fav);
    ////  }
}

// u2_reck_plow(): queue multiple ova (external).
//
void
u2_proc_plow(u2_reck* rec_u, u2_noun ova)
{
  u2_noun ovi = ova;

  while ( u2_nul != ovi ) {
    u2_noun ovo=u2h(ovi);

    u2_proc_plan(rec_u, u2k(u2h(ovo)), u2k(u2t(ovo)));
    ovi = u2t(ovi);
  }
  u2z(ova);
}



void
u2_proc_work(u2_reck* rec_u)
{
  u2_cart* egg_u;
  u2_noun  ova;
  u2_noun  vir;
  u2_noun  nex;

  //  Delete finished events.
  //
  while ( rec_u->ova.egg_u ) {

    egg_u = rec_u->ova.egg_u;

    if ( u2_yes == egg_u->done ) {
      vir = egg_u->vir;

      if ( egg_u == rec_u->ova.geg_u ) {
        c3_assert(egg_u->nex_u == 0);
        rec_u->ova.geg_u = rec_u->ova.egg_u = 0;
      }
      else {
        c3_assert(egg_u->nex_u != 0);
        rec_u->ova.egg_u = egg_u->nex_u;
      }

      egg_u->log = u2_yes;
      free(egg_u);
    }
    else break;
  }

  //  Poke pending events, leaving the poked events and errors on rec_u->roe.
  //
  {
    if ( 0 == u2R->lug_u.len_d ) {
      return;
    }
    ova = u2_ckb_flop(rec_u->roe);     /// call multiple things off queue and punk them to arvo
    rec_u->roe = u2_nul;

    while ( u2_nul != ova ) {
      _proc_punk(rec_u, u2k(u2t(u2h(ova))));   // arvo has got it ; side effects are queued up SOMEWHERE
      c3_assert(u2_nul == u2h(u2h(ova)));

      nex = u2k(u2t(ova));
      u2z(ova); ova = nex;
    }
  }

  //  Cartify, jam, encrypt, log this batch of events. 
  //
  {
    c3_t verbose_t = c3_false;

    c3_d    bid_d;

    u2_noun ovo;

    ova = u2_ckb_flop(rec_u->roe);
    rec_u->roe = u2_nul;

    while ( u2_nul != ova ) {
      ovo = u2k(u2t(u2h(ova)));
      vir = u2k(u2h(u2h(ova)));
      nex = u2k(u2t(ova));
      u2z(ova); ova = nex;

      if ( u2_nul == ovo ) {
        continue;
      }

      // Send ova to logging mechanism. 
      //
      // Note that logging mechanisms are
      //   (a) optimized for low latency of injection
      //   (b) not optimized for latency of completion (bc of underlying constraints)
      //
      // Thus we inject this ova, then get on to the next one.  The
      // log will callback w u2_proc_emit() below, and that's where
      // we do things like emit side-effects.
      //
      if(u2_Host.ops_u.kaf_c){
        bid_d = u2_kafk_push_ova(ovo, vir, LOG_MSG_PRECOMMIT);  
      } else {
        bid_d = u2_egz_push_ova(ovo, vir, LOG_MSG_PRECOMMIT);
      }

      if (c3_true == verbose_t) { fprintf(stdout, "\rlogged item %lli\n", (long long int) bid_d); }


    } // while

  }
}


void u2_proc_emit(c3_d        seq_d,
                  u2_noun     ovo,
                  u2_noun     vir)
{
  u2_reck * rec_u = u2A;

  // we're making a new egg here, that's LIKE ovo
  //
  u2_cart *egg_u        = c3_malloc(sizeof(*egg_u));
  egg_u->nex_u = 0;
  egg_u->log   = u2_no;
  egg_u->done  = u2_no;
  egg_u->vir   = vir;
  egg_u->ent_d = seq_d;

  // then we enqueu this NEW egg
  // again...why?
  if ( 0 == rec_u->ova.geg_u ) {
    c3_assert(0 == rec_u->ova.egg_u);
    rec_u->ova.geg_u = rec_u->ova.egg_u = egg_u;
  }
  else {
    c3_assert(0 == rec_u->ova.geg_u->nex_u);
    rec_u->ova.geg_u->nex_u = egg_u;
    rec_u->ova.geg_u = egg_u;
  }
  _proc_kick_all(rec_u, vir);
  egg_u->done = u2_yes;
}
