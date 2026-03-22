-- Layer: Gold
-- Type: Dimension
-- Description: Source quality dimension capturing data provenance and confidence attributes

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.dim_source_quality` AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      data_source_primary,
      data_source_secondary,
      data_source_type,
      confidence_tier,
      quality_score,
      quality_grade,
      review_flag
  ) AS source_quality_key,

  data_source_primary,
  data_source_secondary,
  data_source_type,
  confidence_tier,
  quality_score,
  quality_grade,
  review_flag

FROM (
  SELECT DISTINCT
    data_source_primary,
    data_source_secondary,
    data_source_type,
    confidence_tier,
    quality_score,
    quality_grade,
    review_flag
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE data_source_primary IS NOT NULL
     OR data_source_secondary IS NOT NULL
     OR data_source_type IS NOT NULL
     OR confidence_tier IS NOT NULL
     OR quality_score IS NOT NULL
     OR quality_grade IS NOT NULL
     OR review_flag IS NOT NULL
);