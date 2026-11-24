package io.flowgate.internal.queue;
import org.junit.jupiter.api.Test; import static org.junit.jupiter.api.Assertions.*;
class BT03_7RepeatedSaturationCyclesTest {
 @Test void repeatedFillDrainCyclesReconcileEveryTime() {
  int cap=25; GlobalAdmissionGate gate=new GlobalAdmissionGate(cap); WorkloadQueue q=BT03TestSupport.queue("cycles",0,cap,gate);
  for(int cycle=0;cycle<250;cycle++){for(int i=0;i<cap+15;i++)BT03TestSupport.submit(q,"cycles",cycle*1000L+i); assertEquals(cap,q.size());assertEquals(cap,gate.held());for(int i=0;i<cap;i++)assertNotNull(q.poll());assertEquals(0,q.size());assertEquals(0,gate.held());assertNull(q.poll());}
 }
}
