# HA validation plan

Run after the single FlinkDeployment is RUNNING and at least one checkpoint has completed.

## Baseline

```powershell
oc get flinkdeployments -n openshift-bridge-1l
oc get pods -n openshift-bridge-1l -o wide
oc get leases -n openshift-bridge-1l
```

Confirm:
- two JobManager pods exist (leader + standby);
- TaskManager pods are running;
- the job is RUNNING;
- checkpoints complete successfully;
- Azure contains checkpoint objects.

## Fail the active JobManager

Identify the active JobManager from logs, then delete only that pod:

```powershell
oc delete pod <ACTIVE-JOBMANAGER-POD> -n openshift-bridge-1l
```

Do not delete the FlinkDeployment or the Azure data.

Observe:

```powershell
oc get pods -n openshift-bridge-1l -w
oc get flinkdeployments -n openshift-bridge-1l -w
```

Expected:
- standby becomes leader;
- the job restarts;
- Flink restores from HA metadata and the last completed checkpoint;
- deployment returns to RUNNING/READY.

A restart is expected; HA does not mean uninterrupted execution.

## Verify state continuity

Send several events with the same `equipment_id` before and after failure. `processed_count_for_equipment` should continue from restored keyed state.

## Savepoint

Trigger a savepoint using the Operator snapshot mechanism or Flink REST API. Record the path and verify it in `flink-savepoints`.

## SNO limitation

A single-node cluster cannot keep a standby available after node failure.
