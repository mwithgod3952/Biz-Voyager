# AGENTS.md

이 저장소에서 작업하는 에이전트는 아래 규칙을 반드시 따른다.

- 수정만 하고 끝내지 말 것
- 환경 설치, 라이브러리 설치, kernel 등록, pytest, doctor, notebook smoke test까지 끝낼 것
- 실패하면 수정 후 재실행할 것
- 완료 보고는 pytest 통과, doctor 통과, notebook 2개 smoke test 통과일 때만 할 것
- 남은 수동 작업은 `.env` 값 입력과 실제 notebook 실행뿐이어야 할 것
