package io.flowgate.internal.queue;
import org.junit.jupiter.api.Test; import static org.junit.jupiter.api.Assertions.*;
class BT03_5RejectionPressureTest {
 @Test void overloadIsRejectedAndAcceptedRecordsRemainVisible() {
  int cap=10; GlobalAdmissionGate gate=new GlobalAdmissionGate(cap); WorkloadQueue q=BT03TestSupport.queue("reject",0,cap,gate);
  int accepted=0,rejected=0; for(int i=0;i<5000;i++){if(BT03TestSupport.submit(q,"reject",i))accepted++;else rejected++;}
  assertEquals(cap,accepted); assertEquals(4990,rejected); assertEquals(cap,q.snapshot().size()); assertEquals(cap,gate.held());
  for(int i=0;i<cap;i++)assertNotNull(q.poll()); assertEquals(0,gate.held());
 }
}
