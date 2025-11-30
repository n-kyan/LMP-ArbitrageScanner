#!/bin/bash
echo "Building LMP Scanner..."
cd lmp_scanner
mkdir -p build
cd build
cmake ..
make

if [ $? -eq 0 ]; then
    echo ""
    echo "Build successful! Running analysis..."
    echo ""
    ./lmp_scanner ../../lmp_data_merged.csv 0.75
else
    echo "Build failed!"
    exit 1
fi