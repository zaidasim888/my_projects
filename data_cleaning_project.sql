SELECT *
FROM layoffs;


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;


INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Removing duplicates
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, `date`)
AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS
(
   SELECT *,
   ROW_NUMBER() OVER(
   PARTITION BY company, location, industry, total_laid_off, `date`,stage, country, funds_raised_millions)
   AS row_num
   FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num >1;

SELECT *
FROM layoffs_staging
WHERE company='Casper';


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


INSERT INTO layoffs_staging2
 SELECT *,
   ROW_NUMBER() OVER(
   PARTITION BY company, location, industry, total_laid_off, `date`,stage, country, funds_raised_millions)
   AS row_num
   FROM layoffs_staging;
   
   SELECT *
   FROM layoffs_staging2;
   
   SELECT *
   FROM layoffs_staging2
   WHERE row_num >1;
   
   
   DELETE 
   FROM layoffs_staging2
   WHERE row_num >1;

-- Standardization

SELECT company,TRIM(company)
FROM layoffs_staging2;




SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;



SELECT country, TRIM(TRAILING '.' FROM country)
FROM  layoffs_staging2;

UPDATE layoffs_staging2
SET country= TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';



SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y') 
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=str_to_date(`date`, '%m/%d/%Y') ;

SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Filling nulls
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
or industry='';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb' ;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL; 

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Deleting columns
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;








