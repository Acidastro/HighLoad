from __future__ import annotations

from datetime import date
from uuid import UUID

from pydantic import BaseModel, EmailStr


class UserRegister(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str
    birthdate: date
    gender: str
    interests: str | None = None
    city: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    id: UUID
    email: str
    first_name: str
    last_name: str
    birthdate: date
    gender: str
    interests: str | None
    city: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class RegisterResponse(BaseModel):
    user_id: UUID
