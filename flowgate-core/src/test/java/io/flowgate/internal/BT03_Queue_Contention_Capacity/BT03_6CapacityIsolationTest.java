package io.flowgate.internal.queue;
import org.junit.jupiter.api.Test; import static org.junit.jupiter.api.Assertions.*;
class BT03_6CapacityIsolationTest {
 @Test void localCapacityIsIndependentWhileGlobalLimitRemainsShared() {
  GlobalAdmissionGate gate=new GlobalAdmissionGate(100); WorkloadQueue[] qs=new WorkloadQueue[4];
  for(int i=0;i<4;i++)qs[i]=BT03TestSupport.queue("w"+i,i,12,gate);
  for(int i=0;i<12;i++)for(int q=0;q<4;q++)BT03TestSupport.submit(qs[q],"w"+q,q*100L+i);
  for(WorkloadQueue q:qs)assertEquals(12,q.size()); assertEquals(48,gate.held());
  for(int i=0;i<4;i++)while(qs[i].poll()!=null){} assertEquals(0,gate.held());
 }
}
