# Makefile for Rust Project


all: check build format lint test

version: 
	cargo --version
check:
	cargo check

build:
	cargo build

format:
	cargo fmt

lint:
	cargo clippy

test:
	cargo test

release:
	cargo build --release