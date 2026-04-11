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


class UserSearchResult(BaseModel):
    id: UUID
    first_name: str
    second_name: str
    birthdate: date
    biography: str | None
    city: str


class PostCreate(BaseModel):
    text: str


class PostUpdate(BaseModel):
    id: UUID
    text: str


class PostIdResponse(BaseModel):
    id: UUID


class Post(BaseModel):
    id: UUID
    text: str
    author_user_id: UUID
