#!/bin/bash

EXAM_ROOT=$(dirname ${BASH_SOURCE})

python ${EXAM_ROOT}/src/anomalous_purchases.py ${EXAM_ROOT}/log_output/flagged_purchases.json ${EXAM_ROOT}/log_input/batch_log.json ${EXAM_ROOT}/log_input/stream_log.json 