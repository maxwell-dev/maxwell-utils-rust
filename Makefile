CARGO=cargo
CARGO_NIGHTLY=rustup run nightly cargo

build:
	${CARGO} build --color=always --all --all-targets

build-nightly:
	${CARGO_NIGHTLY} build --color=always --all --all-targets

release:
	${CARGO} build --release --color=always --all --all-targets && bin/release.sh

release-nightly:
	${CARGO_NIGHTLY} build --release --color=always --all --all-targets && bin/release.sh

test:
	RUST_BACKTRACE=1 ${CARGO} test -- --nocapture

test-nightly:
	RUST_BACKTRACE=1 ${CARGO_NIGHTLY} test -- --nocapture

clean:
	${CARGO} clean
