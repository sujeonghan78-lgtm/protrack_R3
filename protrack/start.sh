#!/bin/bash

# ── PRO-TRACK 실행 스크립트 ──────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo -e "${CYAN}  ██████╗ ██████╗  ██████╗       ████████╗██████╗  █████╗  ██████╗██╗  ██╗${NC}"
echo -e "${CYAN}  ██╔══██╗██╔══██╗██╔═══██╗         ██╔══╝██╔══██╗██╔══██╗██╔════╝██║ ██╔╝${NC}"
echo -e "${CYAN}  ██████╔╝██████╔╝██║   ██║ █████╗  ██║   ██████╔╝███████║██║     █████╔╝ ${NC}"
echo -e "${CYAN}  ██╔═══╝ ██╔══██╗██║   ██║ ╚════╝  ██║   ██╔══██╗██╔══██║██║     ██╔═██╗ ${NC}"
echo -e "${CYAN}  ██║     ██║  ██║╚██████╔╝         ██║   ██║  ██║██║  ██║╚██████╗██║  ██╗${NC}"
echo -e "${CYAN}  ╚═╝     ╚═╝  ╚═╝ ╚═════╝          ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝${NC}"
echo ""
echo -e "${YELLOW}  공정 관리 시스템 v1.0${NC}"
echo ""

# ── 1. Python 패키지 설치 확인 ─────────────────────────────────────────────
echo -e "${CYAN}[1/3]${NC} Python 패키지 확인 중..."
cd "$BACKEND_DIR"

if ! python3 -c "import fastapi, uvicorn, pandas, openpyxl, jose, passlib" 2>/dev/null; then
    echo -e "${YELLOW}  패키지 설치 중...${NC}"
    pip install -r requirements.txt -q
fi
echo -e "${GREEN}  ✓ 패키지 준비 완료${NC}"

# ── 2. FastAPI 백엔드 시작 ──────────────────────────────────────────────────
echo -e "${CYAN}[2/3]${NC} 백엔드 서버 시작 중 (포트 8000)..."
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
sleep 2

if kill -0 $BACKEND_PID 2>/dev/null; then
    echo -e "${GREEN}  ✓ 백엔드 실행 중 (PID: $BACKEND_PID)${NC}"
else
    echo -e "${RED}  ✗ 백엔드 시작 실패${NC}"
    exit 1
fi

# ── 3. 프론트엔드 서버 시작 ─────────────────────────────────────────────────
echo -e "${CYAN}[3/3]${NC} 프론트엔드 서버 시작 중 (포트 3000)..."
cd "$FRONTEND_DIR"
python3 -m http.server 3000 &
FRONTEND_PID=$!
sleep 1
echo -e "${GREEN}  ✓ 프론트엔드 실행 중 (PID: $FRONTEND_PID)${NC}"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  🚀 PRO-TRACK 실행 완료!${NC}"
echo ""
echo -e "  브라우저에서 접속: ${CYAN}http://localhost:3000${NC}"
echo ""
echo -e "  📋 테스트 계정:"
echo -e "  ${YELLOW}관리자${NC}  →  admin / admin1234"
echo -e "  ${YELLOW}뷰어${NC}    →  viewer / viewer1234"
echo ""
echo -e "  API 문서:    ${CYAN}http://localhost:8000/docs${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${YELLOW}  종료하려면 Ctrl+C를 누르세요.${NC}"
echo ""

# ── 종료 핸들러 ────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo -e "${YELLOW}  서버 종료 중...${NC}"
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    echo -e "${GREEN}  ✓ 종료 완료${NC}"
    exit 0
}
trap cleanup SIGINT SIGTERM

wait
