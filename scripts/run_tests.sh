#!/bin/bash

# 设置表情符号
PASS="✅"
FAIL="❌"
RUNNING="🚀"
COVERAGE="📊"
SUMMARY="📝"

# 创建测试结果目录
TEST_RESULTS_DIR="./scripts/test_results"
mkdir -p $TEST_RESULTS_DIR

# 获取当前时间
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# 测试结果文件
SUMMARY_FILE="$TEST_RESULTS_DIR/test_summary_$TIMESTAMP.txt"
DETAILED_FILE="$TEST_RESULTS_DIR/test_detailed_$TIMESTAMP.txt"
COVERAGE_FILE="$TEST_RESULTS_DIR/coverage_$TIMESTAMP.txt"

# 初始化计数器
total_tests=0
passed_tests=0
failed_tests=0

# 打印测试结果
print_result() {
    local package=$1
    local status=$2
    local output=$3
    local coverage=$4
    
    echo -e "\n$RUNNING Testing $package..." | tee -a $DETAILED_FILE
    echo "$output" | tee -a $DETAILED_FILE
    
    if [ "$status" -eq 0 ]; then
        echo -e "$PASS $package tests passed" | tee -a $SUMMARY_FILE
        ((passed_tests++))
    else
        echo -e "$FAIL $package tests failed" | tee -a $SUMMARY_FILE
        ((failed_tests++))
    fi
    
    if [ ! -z "$coverage" ]; then
        echo -e "$COVERAGE Coverage for $package: $coverage" | tee -a $COVERAGE_FILE
    fi
    
    ((total_tests++))
}

# 运行测试并收集结果
run_tests() {
    echo "$RUNNING Starting tests at $(date)" | tee $SUMMARY_FILE
    echo "==========================================" | tee -a $SUMMARY_FILE

    # 运行所有包的测试并收集覆盖率
    echo -e "\n$RUNNING Running all package tests with coverage..."
    go test ./... -v -coverprofile=coverage.out 2>&1 | tee -a $DETAILED_FILE
    
    # 生成覆盖率报告
    go tool cover -func=coverage.out > coverage_report.txt
    overall_coverage=$(grep total: coverage_report.txt | awk '{print $3}')
    echo -e "$COVERAGE Overall coverage: $overall_coverage" | tee -a $COVERAGE_FILE

    # 分别运行各个包的测试
    echo -e "\n$RUNNING Running individual package tests..."
    
    # LSM 包测试
    echo -e "\n$RUNNING Testing LSM package..."
    lsm_output=$(go test ./lsm/... -v -coverprofile=lsm_coverage.out 2>&1)
    lsm_coverage=$(go tool cover -func=lsm_coverage.out | grep total: | awk '{print $3}')
    print_result "LSM" $? "$lsm_output" "$lsm_coverage"

    # File 包测试
    echo -e "\n$RUNNING Testing File package..."
    file_output=$(go test ./file/... -v -coverprofile=file_coverage.out 2>&1)
    file_coverage=$(go tool cover -func=file_coverage.out | grep total: | awk '{print $3}')
    print_result "File" $? "$file_output" "$file_coverage"

    # Utils 包测试
    echo -e "\n$RUNNING Testing Utils package..."
    utils_output=$(go test ./utils/... -v -coverprofile=utils_coverage.out 2>&1)
    utils_coverage=$(go tool cover -func=utils_coverage.out | grep total: | awk '{print $3}')
    print_result "Utils" $? "$utils_output" "$utils_coverage"

    # NoKV 包测试
    echo -e "\n$RUNNING Testing NoKV package..."
    nokv_output=$(go test ./... -v -coverprofile=nokv_coverage.out 2>&1)
    nokv_coverage=$(go tool cover -func=nokv_coverage.out | grep total: | awk '{print $3}')
    print_result "NoKV" $? "$nokv_output" "$nokv_coverage"

    # Benchmark 测试
    echo -e "\n$RUNNING Running benchmarks..."
    benchmark_output=$(go test ./benchmark/... -bench=. -benchmem 2>&1)
    print_result "Benchmark" $? "$benchmark_output"

    # 生成HTML覆盖率报告
    go tool cover -html=coverage.out -o coverage.html

    # 打印总结
    echo -e "\n$SUMMARY === Test Summary ===" | tee -a $SUMMARY_FILE
    echo "Total tests: $total_tests" | tee -a $SUMMARY_FILE
    echo "Passed: $passed_tests" | tee -a $SUMMARY_FILE
    echo "Failed: $failed_tests" | tee -a $SUMMARY_FILE
    echo "Overall coverage: $overall_coverage" | tee -a $SUMMARY_FILE
    echo "Test completed at $(date)" | tee -a $SUMMARY_FILE
    
    # 清理临时文件
    rm -f coverage.out coverage_report.txt lsm_coverage.out file_coverage.out utils_coverage.out
}

# 检查是否安装了必要的工具
check_dependencies() {
    if ! command -v go &> /dev/null; then
        echo -e "$FAIL Error: Go is not installed"
        exit 1
    fi
}

# 主函数
main() {
    check_dependencies
    run_tests
}

# 运行主函数
main