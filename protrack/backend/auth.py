from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional

# ─── Config ──────────────────────────────────────────────────────────────────
SECRET_KEY = "protrack-secret-key-change-in-production-2025"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8 hours

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


# ─── Models ───────────────────────────────────────────────────────────────────
class Token(BaseModel):
    access_token: str
    token_type: str
    role: str
    username: str


class TokenData(BaseModel):
    username: Optional[str] = None
    role: Optional[str] = None


class User(BaseModel):
    username: str
    role: str  # "admin" | "viewer"
    full_name: str


# ─── Fake User DB (실서비스에서는 DB로 교체) ────────────────────────────────
USERS_DB = {
    "admin": {
        "username": "admin",
        "full_name": "관리자",
        "role": "admin",
        "hashed_password": pwd_context.hash("admin1234"),
    },
    "viewer": {
        "username": "viewer",
        "full_name": "뷰어 계정",
        "role": "viewer",
        "hashed_password": pwd_context.hash("viewer1234"),
    },
    "operator": {
        "username": "operator",
        "full_name": "현장 담당자",
        "role": "viewer",
        "hashed_password": pwd_context.hash("op1234"),
    },
}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str) -> Optional[User]:
    user_dict = USERS_DB.get(username)
    if not user_dict:
        return None
    if not verify_password(password, user_dict["hashed_password"]):
        return None
    return User(username=user_dict["username"], role=user_dict["role"], full_name=user_dict["full_name"])


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="인증이 필요합니다.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username, role=role)
    except JWTError:
        raise credentials_exception

    user_dict = USERS_DB.get(token_data.username)
    if user_dict is None:
        raise credentials_exception
    return User(username=user_dict["username"], role=user_dict["role"], full_name=user_dict["full_name"])


async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="관리자 권한이 필요합니다.",
        )
    return current_user
