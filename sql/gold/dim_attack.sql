-- Layer: Gold
-- Type: Dimension
-- Description: Attack dimension capturing attack vectors, chains, and attribution details

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.dim_attack` AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      attack_vector_primary,
      attack_vector_secondary,
      attack_chain,
      attributed_group,
      attribution_confidence
  ) AS attack_key,

  attack_vector_primary,
  attack_vector_secondary,
  attack_chain,
  attributed_group,
  attribution_confidence

FROM (
  SELECT DISTINCT
    attack_vector_primary,
    attack_vector_secondary,
    attack_chain,
    attributed_group,
    attribution_confidence
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE attack_vector_primary IS NOT NULL
     OR attack_vector_secondary IS NOT NULL
     OR attack_chain IS NOT NULL
     OR attributed_group IS NOT NULL
     OR attribution_confidence IS NOT NULL
);