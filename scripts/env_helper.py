#!/usr/bin/env python3
"""
Environment configuration helper for Gamebot project.

This script helps manage environment configurations for different execution contexts:
- Local development (direct database connection)
- Docker/Airflow execution (container networking)
- Production deployment

Usage:
    python scripts/env_helper.py --context local
    python scripts/env_helper.py --context docker
    python scripts/env_helper.py --check
"""

import argparse
import os
from pathlib import Path


def get_project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent


def get_env_config(context="auto"):
    """Get environment configuration for specified context."""

    if context == "auto":
        # Auto-detect context based on environment
        if os.getenv("DOCKER_CONTAINER"):
            context = "docker"
        elif os.getenv("IS_DEPLOYED") == "true":
            context = "prod"
        else:
            context = "local"

    base_config = {
        "DB_NAME": os.getenv("DB_NAME", "survivor_dw_dev"),
        "DB_USER": os.getenv("DB_USER", "survivor_dev"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", "survivor_dev_password"),
        "SURVIVOR_ENV": os.getenv("SURVIVOR_ENV", "dev"),
        "GAMEBOT_TARGET_LAYER": os.getenv("GAMEBOT_TARGET_LAYER", "gold"),
    }

    if context == "local":
        return {
            **base_config,
            "DB_HOST": "localhost",
            "DB_PORT": "5433",
            "CONNECTION_STRING": f"postgresql+psycopg2://{base_config['DB_USER']}:{base_config['DB_PASSWORD']}@localhost:5433/{base_config['DB_NAME']}",
        }
    elif context == "docker":
        return {
            **base_config,
            "DB_HOST": "warehouse-db",
            "DB_PORT": "5432",
            "CONNECTION_STRING": f"postgresql+psycopg2://{base_config['DB_USER']}:{base_config['DB_PASSWORD']}@warehouse-db:5432/{base_config['DB_NAME']}",
        }
    elif context == "prod":
        return {
            **base_config,
            "DB_HOST": os.getenv("PROD_DB_HOST", "localhost"),
            "DB_PORT": os.getenv("PROD_DB_PORT", "5432"),
            "CONNECTION_STRING": f"postgresql+psycopg2://{base_config['DB_USER']}:{base_config['DB_PASSWORD']}@{os.getenv('PROD_DB_HOST', 'localhost')}:{os.getenv('PROD_DB_PORT', '5432')}/{base_config['DB_NAME']}",
        }
    else:
        raise ValueError(f"Unknown context: {context}")


def check_environment():
    """Check current environment configuration."""
    print("=== Environment Configuration Check ===")
    print(f"Project Root: {get_project_root()}")
    print(f"Current Working Directory: {os.getcwd()}")

    # Detect context
    if os.getenv("DOCKER_CONTAINER"):
        context = "docker"
    elif os.getenv("IS_DEPLOYED") == "true":
        context = "prod"
    else:
        context = "local"

    print(f"Detected Context: {context}")

    config = get_env_config(context)
    print("\nConfiguration:")
    for key, value in config.items():
        if "PASSWORD" in key:
            print(f"  {key}: {'*' * len(value)}")
        else:
            print(f"  {key}: {value}")

    # Test database connection
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=config["DB_HOST"],
            port=config["DB_PORT"],
            dbname=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
        )
        conn.close()
        print("\nDatabase connection successful!")
    except Exception as e:
        print(f"\nDatabase connection failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Environment configuration helper")
    parser.add_argument(
        "--context",
        choices=["local", "docker", "prod", "auto"],
        default="auto",
        help="Execution context",
    )
    parser.add_argument(
        "--check", action="store_true", help="Check current environment configuration"
    )

    args = parser.parse_args()

    if args.check:
        check_environment()
    else:
        config = get_env_config(args.context)
        for key, value in config.items():
            print(f"{key}={value}")


if __name__ == "__main__":
    main()
