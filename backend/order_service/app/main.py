import asyncio
import json
import logging
import os
import sys
import time
from decimal import Decimal
from typing import List, Optional

import aio_pika
import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, joinedload

from .db import Base, SessionLocal, engine, get_db
from .models import Order, OrderItem
from .schemas import (
    OrderCreate,
    OrderItemResponse,
    OrderResponse,
    OrderStatusUpdate,
    OrderUpdate,
)

# --- Standard Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Suppress noisy logs from third-party libraries for cleaner output
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

# --- Service URLs Configuration ---
CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:8002")
PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://localhost:8000")

logger.info(
    f"Order Service: Configured to communicate with Customer Service at: {CUSTOMER_SERVICE_URL}"
)
logger.info(
    f"Order Service: Configured to communicate with Product Service at: {PRODUCT_SERVICE_URL}"
)

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# Global RabbitMQ connection and channel objects
rabbitmq_connection: Optional[aio_pika.Connection] = None
rabbitmq_channel: Optional[aio_pika.Channel] = None
rabbitmq_exchange: Optional[aio_pika.Exchange] = None

# --- FastAPI Application Setup ---
app = FastAPI(
    title="Order Service API",
    description="Manages orders for mini-ecommerce app, with synchronous stock deduction.",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- RabbitMQ Helper Functions ---
async def connect_to_rabbitmq():
    global rabbitmq_connection, rabbitmq_channel, rabbitmq_exchange
    rabbitmq_url = (
        f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
    )
    max_retries = 10
    retry_delay_seconds = 5

    for i in range(max_retries):
        try:
            logger.info(
                f"Order Service: Attempting to connect to RabbitMQ (attempt {i+1}/{max_retries})..."
            )
            rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                "ecomm_events", aio_pika.ExchangeType.DIRECT, durable=True
            )
            logger.info(
                "Order Service: Connected to RabbitMQ and declared 'ecomm_events' exchange."
            )
            return True
        except Exception as e:
            logger.warning(f"Order Service: Failed to connect to RabbitMQ: {e}")
            if i < max_retries - 1:
                await asyncio.sleep(retry_delay_seconds)
            else:
                logger.critical(
                    f"Order Service: Failed to connect to RabbitMQ after {max_retries} attempts."
                )
                return False
    return False


async def close_rabbitmq_connection():
    if rabbitmq_connection:
        logger.info("Order Service: Closing RabbitMQ connection.")
        await rabbitmq_connection.close()


async def publish_event(routing_key: str, message_data: dict):
    if not rabbitmq_exchange:
        logger.error("Order Service: RabbitMQ exchange not available. Cannot publish.")
        return
    try:
        message_body = json.dumps(message_data).encode("utf-8")
        message = aio_pika.Message(
            body=message_body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await rabbitmq_exchange.publish(message, routing_key=routing_key)
        logger.info(
            f"Order Service: Published event '{routing_key}' with data: {message_data}"
        )
    except Exception as e:
        logger.error(
            f"Order Service: Failed to publish event '{routing_key}': {e}", exc_info=True
        )

# --- FastAPI Event Handlers ---
@app.on_event("startup")
async def startup_event():
    max_retries = 10
    retry_delay_seconds = 5
    for i in range(max_retries):
        try:
            logger.info(f"Order Service: Attempting DB connect (attempt {i+1})...")
            Base.metadata.create_all(bind=engine)
            logger.info("Order Service: DB connected and tables ensured.")
            break
        except OperationalError as e:
            logger.warning(f"Order Service: DB connect failed: {e}")
            if i < max_retries - 1:
                time.sleep(retry_delay_seconds)
            else:
                logger.critical("Order Service: DB connection failed permanently.")
                sys.exit(1)
        except Exception as e:
            logger.critical(
                f"Order Service: Unexpected DB startup error: {e}", exc_info=True
            )
            sys.exit(1)

    if await connect_to_rabbitmq():
        asyncio.create_task(consume_stock_events(SessionLocal))
    else:
        logger.error("Order Service: RabbitMQ connection failed at startup.")

@app.on_event("shutdown")
async def shutdown_event():
    await close_rabbitmq_connection()

# --- Root Endpoint ---
@app.get("/", status_code=status.HTTP_200_OK)
async def read_root():
    return {"message": "Welcome to the Order Service!"}

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "ok", "service": "order-service"}

# [Keep the rest of your endpoints: create_order, list_orders, get_order, update_order_status, delete_order, get_order_items — unchanged]
