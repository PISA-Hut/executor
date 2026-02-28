#!/bin/bash
#SBATCH --job-name=carla-tyms
#SBATCH --output=outputs/stdout/job_%j
#SBATCH --error=outputs/stderr/job_%j
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=12G

echo "=== SLURM TEST JOB ==="
echo "Cluster:       $SLURM_CLUSTER_NAME"
echo "Job ID:        $SLURM_JOB_ID"
echo "Partition:     $SLURM_JOB_PARTITION"
echo "Job Name:      $SLURM_JOB_NAME"
echo "Node List:     $SLURM_NODELIST"
echo "Submit Dir:    $SLURM_SUBMIT_DIR"
echo "Start Time:    $(date)"
echo

echo "Hostname:      $(hostname)"
echo "CPU Info:"
echo

echo "Start Time:    $(date)"

CARLA_ROOT=${HOME}/carla-0.9.16/
CARLA_PORT=$((SLURM_STEP_GPUS*10 + 2000))
${CARLA_ROOT}/CarlaUE4.sh -renderoffscreen -nosound -quality-level=Low -fps=1 -graphicsadaptor=$SLURM_STEP_GPUS -carla-port=$CARLA_PORT
CARLA_PORT=$CARLA_PORT uv run -m worker --av carla-agent

if [ $? -ne 0 ]; then
    echo "Worker execution failed."
    exit 1
fi

echo "End Time:      $(date)"
