package io.flowgate.internal.queue;
import java.util.Random; import org.junit.jupiter.api.Test; import static org.junit.jupiter.api.Assertions.*;
class BT03_8RandomizedQueueAbuseTest {
 @Test void seededRandomSubmitPollRemoveMaintainsBoundsAndAccounting() {
  int cap=48; GlobalAdmissionGate gate=new GlobalAdmissionGate(cap); WorkloadQueue q=BT03TestSupport.queue("random",0,cap,gate); Random r=new Random(0xB703L); long id=0;
  for(int i=0;i<200_000;i++){int op=r.nextInt(100); if(op<62)BT03TestSupport.submit(q,"random",id++); else if(op<88)q.poll(); else {var snap=q.snapshot(); if(!snap.isEmpty())q.remove(snap.get(r.nextInt(snap.size())));} assertTrue(q.size()>=0&&q.size()<=cap,"local bound at iteration "+i); assertTrue(gate.held()>=0&&gate.held()<=cap,"global bound at iteration "+i); assertEquals(q.size(),gate.held());}
  while(q.poll()!=null){} assertEquals(0,q.size());assertEquals(0,gate.held());
 }
}
