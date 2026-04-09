# ARIS Dashboard Validation Framework

This framework validates that your AI NLP-to-SQL system produces **accurate results** that match the source ARIS dashboard data.

## 📊 Overview

The validation system:
1. **Queries your AI system** with natural language questions
2. **Compares results** against ARIS dashboard ground truth
3. **Generates visual reports** showing matches/mismatches
4. **Tracks accuracy** over time as you improve the system

## 🚀 Quick Start

### Step 1: Add ARIS Ground Truth Data

Edit `ground_truth.csv` and add actual values from your ARIS dashboard:

```csv
test_case_id,query,expected_value,tolerance,unit,source,expected_categories,expected_top_reason
otif_2024,What is OTIF in 2024?,85.3,0.5,percentage,ARIS Dashboard > KPIs > OTIF > 2024,,
```

**How to get values from ARIS:**
1. Open your ARIS dashboard
2. Navigate to the KPI/metric
3. Copy the exact value
4. Add to CSV with appropriate tolerance

### Step 2: Run Validation Tests

```bash
# From project root (j&j/)
cd tests

# Run all validation tests
pytest test_aris_validation.py -v

# Run and generate HTML report
pytest test_aris_validation.py -v --html=../reports/validation.html --self-contained-html

# Run specific test
pytest test_aris_validation.py::TestARISValidation::test_numeric_kpi_accuracy -v
```

### Step 3: View Results

```bash
# Open the HTML report in browser
start ../reports/validation.html  # Windows
open ../reports/validation.html   # Mac
```

## 📝 Ground Truth CSV Format

### Numeric KPI Tests

For numeric metrics (OTIF, warehouse utilization, counts):

```csv
test_case_id,query,expected_value,tolerance,unit,source,expected_categories,expected_top_reason
otif_2024,What is OTIF in 2024?,85.3,0.5,percentage,ARIS Dashboard > KPIs > OTIF > 2024,,
shipments_count,How many shipments in 2024?,12450,10,count,ARIS Dashboard > Shipments > Total > 2024,,
```

**Columns:**
- `test_case_id`: Unique identifier for the test
- `query`: Natural language question to ask the AI
- `expected_value`: Exact value from ARIS dashboard
- `tolerance`: Allowed difference (e.g., ±0.5 for percentages, ±10 for counts)
- `unit`: percentage, count, days, etc.
- `source`: Where in ARIS dashboard you found this value
- `expected_categories`: Leave empty for numeric tests
- `expected_top_reason`: Leave empty for numeric tests

### Categorical Tests

For categorical data (delay reasons, warehouse issues):

```csv
test_case_id,query,expected_value,tolerance,unit,source,expected_categories,expected_top_reason
delivery_delays_reasons,Give me reasons for delivery delays,,,categorical,ARIS Dashboard > Deliveries > Delay Reasons,"Carrier Issue|Weather Disruption|Traffic Congestion|Warehouse Delay|Route Disruption",Carrier Issue
```

**Columns:**
- `expected_value`: Leave empty
- `tolerance`: Leave empty
- `unit`: Set to "categorical"
- `expected_categories`: Pipe-separated list of all categories that should appear
- `expected_top_reason`: The most common category (should appear first in results)

## 🎯 Validation Test Types

### 1. Numeric KPI Accuracy Tests

Validates that numeric values match ARIS within tolerance:

```python
def test_numeric_kpi_accuracy(self, test_case_key):
    # Queries AI system
    # Extracts numeric result
    # Compares to ARIS ground truth
    # Fails if difference > tolerance
```

**Example:**
- Query: "What is OTIF in 2024?"
- ARIS Value: 85.3%
- AI Result: 85.1%
- Difference: 0.2% ✅ PASS (tolerance ±0.5%)

### 2. Categorical Data Tests

Validates that all expected categories are present and top category matches:

```python
def test_delay_reasons_categories(self):
    # Queries AI system
    # Extracts list of categories
    # Validates all expected categories present
    # Validates top category matches ARIS
```

**Example:**
- Query: "Give me reasons for delivery delays"
- ARIS Categories: Carrier Issue (most common), Weather, Traffic
- AI Categories: Carrier Issue, Weather, Traffic ✅ PASS

### 3. SQL Correctness Tests

Validates that generated SQL uses the correct source tables:

```python
def test_sql_uses_correct_tables(self):
    # Checks SQL uses expected tables
    # Checks SQL doesn't use forbidden tables
```

## 📊 Understanding the Report

The HTML report shows:

### Summary Section
- **Total Tests**: Number of validation tests run
- **Passed**: Tests where AI matched ARIS
- **Failed**: Tests where AI didn't match ARIS
- **Pass Rate**: Overall accuracy percentage

### Test Results
Each test shows:
- ✅ **PASSED** or ❌ **FAILED** status
- **Query**: The natural language question
- **Expected vs Actual**: ARIS value vs AI result
- **Difference**: How far off (with tolerance)
- **ARIS Source**: Where the ground truth came from
- **Generated SQL**: What SQL the AI created

## 🔄 Workflow

### Initial Setup
1. Extract key metrics from ARIS dashboard
2. Add to `ground_truth.csv`
3. Run validation tests
4. See which queries work vs fail

### Iterative Improvement
1. Fix failing tests by improving prompts/schema
2. Re-run validation
3. Track improvement over time
4. Add more test cases as you go

### Before Production
1. Ensure pass rate > 95%
2. Review all failed tests
3. Document any acceptable differences
4. Get stakeholder sign-off on accuracy

## 📝 Adding New Test Cases

### For a new KPI:

1. **Get ARIS value**:
   - Open ARIS dashboard
   - Navigate to: KPIs > Warehouse > Utilization > Mumbai > 2024
   - Value: 82.5%

2. **Add to CSV**:
   ```csv
   warehouse_util_mumbai_2024,What is warehouse utilization in Mumbai in 2024?,82.5,1.0,percentage,ARIS Dashboard > Warehouse > Mumbai > 2024,,
   ```

3. **Run test**:
   ```bash
   pytest test_aris_validation.py -v
   ```

4. **If it fails**:
   - Check the generated SQL
   - Verify database has the data
   - Adjust SQL agent prompts if needed
   - Re-run

## 🐛 Troubleshooting

### Test fails with "Could not extract numeric value"
- **Cause**: AI returned categorical data instead of numeric
- **Fix**: Check the generated SQL; may be using wrong aggregation

### Test fails with large difference
- **Cause**: Database data doesn't match ARIS, or wrong table/column
- **Fix**:
  1. Query database directly to verify value
  2. Check if you're using correct table
  3. Verify data was synced from ARIS recently

### Test fails with missing categories
- **Cause**: Database doesn't have all delay reasons ARIS has
- **Fix**: Check data pipeline from ARIS to Supabase

### All tests fail with API errors
- **Cause**: Groq API key invalid or missing
- **Fix**: Update GROQ_API_KEY in `.env`

## 📈 Tracking Improvements

Save reports over time:

```bash
# Run tests weekly
pytest test_aris_validation.py --html=../reports/validation_week1.html
pytest test_aris_validation.py --html=../reports/validation_week2.html
```

Compare reports to see:
- Pass rate improving
- Which queries still fail
- Where to focus effort

## 🎯 Target Metrics

**Good System:**
- Pass rate > 95%
- All core KPIs passing
- Max 5% difference on numeric values

**Production-Ready:**
- Pass rate > 98%
- All critical KPIs 100% accurate
- Stakeholder validated results

## 💡 Tips

1. **Start small**: Begin with 5-10 most important KPIs
2. **Expand gradually**: Add more test cases as you validate
3. **Update regularly**: Re-validate when you change prompts or schema
4. **Document exceptions**: If ARIS and DB intentionally differ, note why
5. **Automate**: Run validation tests in CI/CD pipeline

## 📞 Support

If you need help:
1. Check the test output for specific error messages
2. Review generated SQL to see what query was created
3. Query database directly to verify data exists
4. Check ARIS dashboard to confirm ground truth value

---

**Remember**: The goal is to ensure your AI system is **trustworthy** and produces **accurate results** that stakeholders can rely on for business decisions.
