# # ANP Fuel Sales ETL Test
🗣📖 This repository contains my personal solution for solving the "ANP Fuel Sales ETL Test" case proposed by Raízen, an integrated energy company of Brazilian origin with presence in the sugar and ethanol production, fuel distribution and power generation sectors. The case proposed by Raizen has the objective to extract internal pivot caches from consolidated reports [made available](http://www.anp.gov.br/dados-estatisticos) by Brazilian government's regulatory agency for oil/fuels, _ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)_.
<hr>

## 💀🏴‍☠️ Goals

The proposed objective is to extract data from tables like the following from `xls` file:
 
![Pivot Table](https://github.com/raizen-analytics/data-engineering-test/raw/master/images/pivot.png)

The developed pipeline is meant to extract and structure the underlying data of two of these tables:

-   Sales of oil derivative fuels by UF and product
-   Sales of diesel by UF and type
 ## 🎲📊 Schema

Data should be stored in the following format:

Column

Type

`year_month`

`date`

`uf`

`string`

`product`

`string`

`unit`

`string`

`volume`

`double`

`created_at`

`timestamp`

Remember to define a convenient partitioning or indexing schema.

## 💻👩‍💻 How to execute it
`git clone https://github.com/FelipeGaleao/etl-raizen`
`cd etl-raizen`
`docker-compose up --build .`
`access on browser http://localhost:8080`

## 🏴‍☠️⚔ Thanks for opportunity!