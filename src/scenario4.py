import great_expectations as gx

# Create an Expectation Suite
expectation_suite_name = "party_data_suite"
suite = gx.ExpectationSuite(name = expectation_suite_name)

# Expectations
# - `party_key` is non-NULL and unique
# - `country` is within an allowed set
# - `source_updated_at` is a valid timestamp
# - `dob`, if present, must be before today
# - Null percentage of `name` is below a defined threshold

# Add Expectations
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column = "party_key")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column = "party_key")
)

# Assumption: Allowed set of `country` is ["IN", "SG", "US", "UK"]
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(column = "country", value_set = ["IN", "SG", "US", "UK"])
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(column = "source_updated_at", strftime_format = "%Y-%m-%d %H:%M:%S")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column = "dob", max_value = "today", strict_max = True)
)

# Assumption: Null percentage of `name` should be below 10%
suite.add_expectation(
    gx.expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(column = "name", min_value = 0.9, max_value = 1.0)
)


# Add the Expectation Suite to the Context
context.suites.add(suite)


# Validate the Data Against the Suite
# Assumption: `batch` is the source dataset definition, which has not been defined here in scope
validation_results = batch.validate(suite)

# Evaluate the Results
print(validation_results)