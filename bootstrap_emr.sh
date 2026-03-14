#!/bin/bash
# ============================================================
# EMR Bootstrap Action: Install PostgreSQL JDBC Driver
# ============================================================
# This script runs on ALL EMR nodes before Spark starts.
# It downloads the PostgreSQL JDBC JAR so spark-submit can
# reference it without embedding it in the job package.
#
# Place this script in S3 and reference it in the EMR
# BootstrapActions configuration.
# ============================================================

set -euo pipefail

JDBC_VERSION="42.7.3"
JDBC_JAR="postgresql-${JDBC_VERSION}.jar"
JDBC_URL="https://jdbc.postgresql.org/download/${JDBC_JAR}"
DEST_DIR="/usr/lib/spark/jars"

echo "[bootstrap] Downloading PostgreSQL JDBC driver ${JDBC_VERSION} …"
sudo wget -q "${JDBC_URL}" -O "${DEST_DIR}/${JDBC_JAR}"
echo "[bootstrap] JDBC driver installed at ${DEST_DIR}/${JDBC_JAR}"

# Install boto3 (should already be present on EMR 6.x, but ensure version)
echo "[bootstrap] Ensuring boto3 is up-to-date …"
sudo pip3 install --quiet --upgrade boto3

echo "[bootstrap] Bootstrap complete."
