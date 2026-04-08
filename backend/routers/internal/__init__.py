from fastapi import APIRouter, Depends

# --- Internal auth dependency ---
# No-op today — Docker Compose network provides isolation.
# Future: validate X-Internal-Token header against an env var,
# or use cloud IAM/mTLS depending on deployment platform.
def verify_internal_caller():
    pass

internal_router = APIRouter(
    prefix="/internal",
    dependencies=[Depends(verify_internal_caller)],
)
