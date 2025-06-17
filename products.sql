CREATE TABLE ProductsProfile (
  product_id STRING(64) NOT NULL,
  product_name STRING(100),
  category STRING(50),
  price FLOAT64,
  manufacturer_id STRING(64),
) PRIMARY KEY(product_id);

CREATE TABLE ProductFeatures (
  product_id STRING(64) NOT NULL,
  feature_id STRING(64) NOT NULL,
  name STRING(100),
  value STRING(100),
) PRIMARY KEY(product_id, feature_id),
  INTERLEAVE IN PARENT ProductsProfile ON DELETE CASCADE;
  
CREATE TABLE Manufacturers (
  manufacturer_id STRING(64) NOT NULL,
  name STRING(100),
  country STRING(50)
) PRIMARY KEY(manufacturer_id);
