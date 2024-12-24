-- Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM layoffs;

-- These are the steps that I took while cleaning this data set.

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns


-- To start, I created a staging data set, as a way to create a back-up of our RAW data and to create the working data
CREATE TABLE layoffs_staging
LIKE layoffs;


INSERT layoffs_staging
SELECT *
FROM layoffs;

-- one way to remove duplicates is to give each row a row number

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- once each row is given a number, let's take a look at a company to confirm whether they are duplicates or not.
-- in this case we looked at casper

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- in order to remove our found dupes, we should create another table in which the row_num is an actual column we can delete.
-- more specifically, we are deleting row numbers over 2

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
FROM layoffs_staging;

-- let's now delete the rows

DELETE
FROM layoffs_staging2
WHERE row_num > 1;







-- Standardize Data

SELECT *
FROM layoffs_staging2;

-- here we are removing any white spaces, and updating the column values.

SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- looking at industry, we can see some industry names are repeated like 'crypto', let's fix  that

SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- similarly, there are two United States, however, one had an extra character, so that must be fixed

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- next, when we imported the data, the 'date' column was shown to be imported as 'text' format
-- let's fix this, by using the STR_TO_DATE function

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y/');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- once are basic formatting issues are fixed, let's tackle the null and blank rows
-- specifically from the industry column

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Bally seems good, so no changed needed
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like there are 2 airbnb's but one is not populated and one is a travel industry
-- given that both are the same location, we can be sure that this is the same company
-- let's write a query that if there is another row with the same name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should first set every blank to null, since this would make it easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now let's check if they are all null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
ORDER BY industry;

-- now let's try to populate these nulls
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- if we check it looks like Bally's was the only one without a populated row to populate the null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's look at the rest of the null values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. 


-- now it's time to remove any data we can't use

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- deleting the rows we can't really use
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;