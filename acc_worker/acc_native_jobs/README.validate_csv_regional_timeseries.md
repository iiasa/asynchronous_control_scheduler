## `root_schema_declarations`
1. `time_dimension`     pointer to the column representing time dimension in the csv timeseries dataset.
2. `unit_dimension`     pointer to the column representing unit dimension in the csv timeseries dataset.
3. `value_dimenstion`   pointer to the column representing value dimension in the csv timeseries dataset.
4. `region_dimension`   pointer to the column representing region dimension in the csv timeseries dataset.
5. `variable_dimension` pointer to the column representing variable dimension th the csv timeseries dataset
6. `final_dimensions_order` list of all dimensions including both columns present in the input csv timesereis dataset and [extra dimension](#extra_dimension) presented by template itself. During validation, if the extra dimension value is already present then it is replaced. 


## Extra Dimension
1. Extra dimensions are refferred to the dimension not defined explicitly as properties in root json schema. Instead, directly mapping object is provided with the key `map_<dimension_name>` in schema json.