from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from app.auth import create_access_token, hash_password, verify_password
from app.database import Database
from app.models import (
    RegisterResponse,
    TokenResponse,
    UserLogin,
    UserRegister,
    UserResponse,
)

router = APIRouter()


@router.post("/user/register", response_model=RegisterResponse)
async def register_user(user: UserRegister) -> RegisterResponse:
    async with Database.connection() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM users WHERE email = $1",
            user.email,
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered",
            )

        password_hash = hash_password(user.password)

        row = await conn.fetchrow(
            """
            INSERT INTO users (email, password_hash, first_name, last_name,
                             birthdate, gender, interests, city)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            """,
            user.email,
            password_hash,
            user.first_name,
            user.last_name,
            user.birthdate,
            user.gender,
            user.interests,
            user.city,
        )

        if row is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create user",
            )

        return RegisterResponse(user_id=row["id"])


@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserLogin) -> TokenResponse:
    async with Database.connection() as conn:
        row = await conn.fetchrow(
            "SELECT id, password_hash FROM users WHERE email = $1",
            credentials.email,
        )

        if not row or not verify_password(credentials.password, row["password_hash"]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )

        token = create_access_token(str(row["id"]))
        return TokenResponse(access_token=token)


@router.get("/user/get/{user_id}", response_model=UserResponse)
async def get_user(user_id: UUID) -> UserResponse:
    async with Database.connection() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, email, first_name, last_name, birthdate,
                   gender, interests, city
            FROM users WHERE id = $1
            """,
            user_id,
        )

        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )

        return UserResponse(
            id=row["id"],
            email=row["email"],
            first_name=row["first_name"],
            last_name=row["last_name"],
            birthdate=row["birthdate"],
            gender=row["gender"],
            interests=row["interests"],
            city=row["city"],
        )
