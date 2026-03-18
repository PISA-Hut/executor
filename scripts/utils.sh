#!/bin/bash

function prologue() {
    echo "=== SLURM TEST JOB ==="
    echo "Cluster:       $SLURM_CLUSTER_NAME"
    echo "Job ID:        $SLURM_JOB_ID"
    echo "Array Job ID:  $SLURM_ARRAY_JOB_ID"
    echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
    echo "Partition:     $SLURM_JOB_PARTITION"
    echo "Job Name:      $SLURM_JOB_NAME"
    echo "Node List:     $SLURM_NODELIST"
    echo "Submit Dir:    $SLURM_SUBMIT_DIR"
    echo "Start Time:    $(date)"
    echo
}


function epilogue() {
    echo
    echo "=== SLURM TEST JOB END ==="
    echo "End Time:      $(date)"
}
