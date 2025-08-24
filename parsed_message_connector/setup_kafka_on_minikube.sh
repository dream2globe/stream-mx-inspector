#!/bin/bash

# ANSI Color Codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print messages
print_message() {
    echo -e "\n${GREEN}==> $1${NC}\n"
}

# Stop script on any error
set -e

# --- 1단계: 개발 환경 준비 (Minikube & Docker) ---
print_message "1단계: 개발 환경 준비 시작"

print_message "Minikube 클러스터 시작 (CPUs: 8, Memory: 16g)"
minikube start --cpus=8 --memory=16g
minikube status

print_message "프로젝트 디렉토리로 이동 및 기존 데이터 정리"
# !!! 스크립트 사용자 환경에 맞게 경로를 수정해주세요 !!!
PROJECT_DIR="$HOME/workspace/project/test-data-pipeline" 
MINIO_DATA_DIR="$HOME/data/minio"

cd "$PROJECT_DIR"
echo "현재 디렉토리: $(pwd)"

if [ "$(docker-compose ps -q | wc -l)" -gt 0 ]; then
    echo "실행 중인 Docker Compose 컨테이너 종료 및 삭제..."
    docker compose down
fi

if [ -d "$MINIO_DATA_DIR" ]; then
    echo "기존 Minio 데이터 삭제..."
    sudo rm -rf "$MINIO_DATA_DIR"
fi
mkdir -p "$MINIO_DATA_DIR"
echo "Minio 데이터 디렉토리 생성 완료: $MINIO_DATA_DIR"

print_message "Kafka Broker 및 Minio 실행"
docker compose up -d
docker compose ps

# --- 2단계: 쿠버네티스 기본 리소스 배포 ---
print_message "2단계: 쿠버네티스 기본 리소스 배포 시작"

print_message "kafka 네임스페이스 생성"
if ! kubectl get namespace kafka > /dev/null 2>&1; then
    kubectl create namespace kafka
else
    echo "kafka 네임스페이스가 이미 존재합니다."
fi
kubectl get namespace kafka

print_message "Schema Registry 배포"
kubectl apply -f schema-registry.yaml -n kafka
echo -e "${YELLOW}Schema Registry Pod가 Running 상태가 될 때까지 30초 대기...${NC}"
sleep 30
kubectl get pods -n kafka --field-selector=status.phase=Running

# --- 3단계: Strimzi Operator 설치 ---
print_message "3단계: Strimzi Operator 설치 시작"

print_message "Strimzi Operator 0.47.0 설치"
helm install strimzi-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 0.47.0 \
  -n kafka \
  --wait # 설치가 완료될 때까지 대기

print_message "Strimzi CRD 적용"
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.47.0/strimzi-crds-0.47.0.yaml -n kafka

echo -e "${YELLOW}Strimzi Operator Pod가 Running 상태가 될 때까지 30초 대기...${NC}"
sleep 30
kubectl get pods -n kafka -l name=strimzi-cluster-operator

# --- 4단계: Kafka Connect 클러스터 배포 ---
print_message "4단계: Kafka Connect 클러스터 배포 시작"

print_message "커스텀 Docker 이미지 빌드"
docker build -t custom-connect-s3:20250824 .
docker images | grep custom-connect-s3

print_message "빌드한 이미지를 Minikube로 로드"
minikube image load custom-connect-s3:20250824

print_message "Kafka Connect 클러스터 배포"
kubectl apply -f kafka-connect.yaml -n kafka

echo -e "${YELLOW}Kafka Connect 클러스터 Pod들이 Running 상태가 될 때까지 90초 대기...${NC}"
sleep 90
kubectl get pods -n kafka -l strimzi.io/cluster=my-connect-cluster

# --- 5단계: AKHQ 관리 도구 설치 ---
print_message "5단계: AKHQ 관리 도구 설치 시작"

print_message "AKHQ Helm 레포지토리 추가 및 설치"
if ! helm repo list | grep -q "akhq"; then
    helm repo add akhq https://akhq.io/
fi
helm install akhq akhq/akhq --namespace kafka -f akhq-values.yaml --wait

echo -e "${YELLOW}AKHQ Pod가 Running 상태가 될 때까지 30초 대기...${NC}"
sleep 30
kubectl get pods -n kafka -l app.kubernetes.io/name=akhq

# --- 최종 안내 ---
print_message "모든 설치 및 배포가 완료되었습니다!"
echo -e "${YELLOW}아래 명령어를 직접 실행하여 AKHQ 대시보드에 접속하세요.${NC}"

echo "
export POD_NAME=\$(kubectl get pods --namespace kafka -l \"app.kubernetes.io/name=akhq,app.kubernetes.io/instance=akhq\" -o jsonpath=\"{.items[0].metadata.name}\")
kubectl port-forward --namespace kafka \$POD_NAME 8080:8080
"