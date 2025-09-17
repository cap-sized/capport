set -e
cargo build

nohup ./target/debug/cp_demo --config ./config/ --output ./output/ --runner demo-run-once --pipeline game_info --execute > ./output/run_game_info.log 2>&1 &

nohup ./target/debug/cp_demo --config ./config/ --output ./output/ --runner demo-run-loop --pipeline game_day --execute > ./output/run_game_info.log 2>&1 &
