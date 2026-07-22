import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import { Text, Instances, Instance } from '@react-three/drei';
import { useStore, Stage } from '@/store';
import * as THREE from 'three';

const STAGE_POSITIONS: Record<Stage, [number, number, number]> = {
  'PO_CREATED': [-8, 0, 0],
  'VENDOR_CONFIRMED': [-4, 0, 0],
  'APPROVED': [0, 0, 0],
  'IN_TRANSIT': [4, 0, 0],
  'RECEIVED': [8, 0, 0],
};

const STAGE_COLORS: Record<Stage, string> = {
  'PO_CREATED': '#3b82f6',
  'VENDOR_CONFIRMED': '#10b981',
  'APPROVED': '#f59e0b',
  'IN_TRANSIT': '#8b5cf6',
  'RECEIVED': '#ec4899',
};

const STAGE_LABELS: Record<Stage, string> = {
  'PO_CREATED': 'PO Created',
  'VENDOR_CONFIRMED': 'Vendor Confirmed',
  'APPROVED': 'Approved',
  'IN_TRANSIT': 'In Transit',
  'RECEIVED': 'Received',
};

export function NetworkGraph() {
  const events = useStore(state => state.events);
  const watermark = useStore(state => state.watermark);
  
  const meshRef = useRef<THREE.InstancedMesh>(null);
  const failMeshRef = useRef<THREE.InstancedMesh>(null); // Different shape for failures

  const dummy = useMemo(() => new THREE.Object3D(), []);
  const color = useMemo(() => new THREE.Color(), []);

  // Stable slot index per aggregate_id to avoid scrambling
  const slotOf = useMemo(() => new Map<string, number>(), []);
  const yOffsets = useMemo(() => new Map<string, number>(), []);

  useFrame((state, delta) => {
    if (!meshRef.current || !failMeshRef.current) return;

    const visibleEvents = events.filter(e => BigInt(e.sequence_engine_key) <= watermark);

    let successCount = 0;
    let failCount = 0;

    for (let i = 0; i < visibleEvents.length; i++) {
      const ev = visibleEvents[i];
      const targetPos = STAGE_POSITIONS[ev.stage] || [0, 0, 0];

      // Assign stable slot
      const slot = slotOf.get(ev.aggregate_id) ?? slotOf.set(ev.aggregate_id, slotOf.size).get(ev.aggregate_id)!;
      
      if (!yOffsets.has(ev.aggregate_id)) {
        yOffsets.set(ev.aggregate_id, (Math.random() - 0.5) * 4);
      }
      const yOff = yOffsets.get(ev.aggregate_id)!;

      const isFail = ev.status === 'FAILURE' || ev.metrics?.sla_breached;
      const targetRef = isFail ? failMeshRef : meshRef;
      const countIndex = isFail ? failCount++ : successCount++;

      targetRef.current!.getMatrixAt(slot, dummy.matrix);
      const currentPos = new THREE.Vector3().setFromMatrixPosition(dummy.matrix);
      
      if (currentPos.lengthSq() < 0.001 && targetPos[0] !== 0) {
        dummy.position.set(targetPos[0], yOff, targetPos[2]);
      } else {
        dummy.position.copy(currentPos).lerp(new THREE.Vector3(targetPos[0], yOff, targetPos[2]), delta * 5);
      }

      dummy.updateMatrix();
      
      // We still update the slot matrix for persistence
      targetRef.current!.setMatrixAt(slot, dummy.matrix);
      
      const targetColor = isFail ? '#ef4444' : (STAGE_COLORS[ev.stage] || '#ffffff');
      targetRef.current!.setColorAt(slot, color.set(targetColor));
    }

    // Hide unused slots by setting their scale to 0 (a simple approach)
    for (let s = 0; s < slotOf.size; s++) {
      // It's easier to just manage the active slots, but since we are splitting across two meshes based on status:
      // A more robust approach for InstancedMesh with changing states is to sync ALL slots and scale down inactive.
      // But for this fix, we just ensure the buffers are marked for update.
    }
    
    // Instead of complex scaling, if we want to separate shapes, 
    // let's simplify: 1 mesh for spheres (success), 1 mesh for boxes (failures)
    // We update all slots for both, and scale to 0 if status doesn't match.
    for (let s = 0; s < slotOf.size; s++) {
      const ev = visibleEvents.find(e => slotOf.get(e.aggregate_id) === s);
      if (!ev) continue;
      
      const isFail = ev.status === 'FAILURE' || ev.metrics?.sla_breached;
      
      // Update Sphere Mesh (Success)
      meshRef.current.getMatrixAt(s, dummy.matrix);
      if (isFail) dummy.scale.setScalar(0);
      else dummy.scale.setScalar(1).lerp(new THREE.Vector3(1,1,1), delta);
      dummy.updateMatrix();
      meshRef.current.setMatrixAt(s, dummy.matrix);

      // Update Box Mesh (Failure)
      failMeshRef.current.getMatrixAt(s, dummy.matrix);
      if (!isFail) dummy.scale.setScalar(0);
      else dummy.scale.setScalar(1).lerp(new THREE.Vector3(1,1,1), delta);
      dummy.updateMatrix();
      failMeshRef.current.setMatrixAt(s, dummy.matrix);
    }

    meshRef.current.count = slotOf.size;
    meshRef.current.instanceMatrix.needsUpdate = true;
    if (meshRef.current.instanceColor) meshRef.current.instanceColor.needsUpdate = true;

    failMeshRef.current.count = slotOf.size;
    failMeshRef.current.instanceMatrix.needsUpdate = true;
    if (failMeshRef.current.instanceColor) failMeshRef.current.instanceColor.needsUpdate = true;
  });

  return (
    <group>
      <ambientLight intensity={0.6} />
      <directionalLight position={[5, 10, 5]} intensity={1.5} />
      
      {/* Background topology graph lines */}
      <mesh position={[-4, 0, -1]}>
        <boxGeometry args={[4, 0.1, 0.1]} />
        <meshBasicMaterial color="#334155" />
      </mesh>
      <mesh position={[4, 0, -1]}>
        <boxGeometry args={[4, 0.1, 0.1]} />
        <meshBasicMaterial color="#334155" />
      </mesh>

      {/* Stage Labels */}
      {Object.entries(STAGE_POSITIONS).map(([stage, pos]) => (
        <Text 
          key={stage} 
          position={[pos[0], 5, pos[2]]} 
          fontSize={0.6}
          color="#94a3b8"
          anchorX="center"
          anchorY="middle"
        >
          {STAGE_LABELS[stage as Stage]}
        </Text>
      ))}

      {/* Success Sphere Nodes */}
      <instancedMesh ref={meshRef} args={[undefined, undefined, 2000]}>
        <sphereGeometry args={[0.3, 16, 16]} />
        <meshStandardMaterial roughness={0.6} metalness={0.1} />
      </instancedMesh>

      {/* Failure Box Nodes */}
      <instancedMesh ref={failMeshRef} args={[undefined, undefined, 2000]}>
        <boxGeometry args={[0.5, 0.5, 0.5]} />
        <meshStandardMaterial roughness={0.6} metalness={0.1} />
      </instancedMesh>
    </group>
  );
}
