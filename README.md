$ env LISTEN=127.0.0.1:8080 DATA_PATH=/home/user/prj/tmp/tdata cargo run
$ cargo build --release --target=x86_64-unknown-linux-musl
$ docker build --no-cache -t travels_010 .
$ docker run --rm -p 8080:80 -v /home/user/prj/tmp/travel-task-data/data:/tmp/data --rm -t travels_010
