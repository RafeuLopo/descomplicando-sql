-- Databricks notebook source
-- 1. Quais são os Top 5 vendedores campeões de vendas de cada UF?

WITH tb_vendedor_dados AS (

  SELECT idVendedor,
         count(DISTINCT idPedido) AS pedidos,
         sum(vlPreco) AS receita

  FROM silver.olist.item_pedido

  GROUP BY idVendedor

),
tb_vendedor_UF AS (

  SELECT t2.*,
         t1.descUF

  FROM silver.olist.vendedor AS t1

  INNER JOIN tb_vendedor_dados AS t2
  ON t1.idVendedor = t2.idVendedor

),
tb_final AS (
  
  SELECT *,
        row_number() OVER (PARTITION BY descUF ORDER BY receita DESC) AS rnReceita
  FROM tb_vendedor_UF

)

SELECT *

FROM tb_final

WHERE rnReceita <= 5

ORDER BY descUF, receita DESC

-- COMMAND ----------

-- 2. Quais são os Top 5 vendedores campeões de vendas em cada categoria?

WITH tb_vendedor_categoria AS (

  SELECT t1.idVendedor,
         t1.idProduto,
         t1.vlPreco,
         t2.descCategoria

  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

),
tb_vendedor AS (

  SELECT idVendedor,
         descCategoria,
         sum(vlPreco) AS receita
         
  FROM tb_vendedor_categoria

  WHERE descCategoria IS NOT null

  GROUP BY idVendedor, descCategoria

),
tb_final AS (

  SELECT *,
        row_number() OVER (PARTITION BY descCategoria ORDER BY receita DESC) AS rnReceita

  FROM tb_vendedor

)

SELECT *

FROM tb_final

WHERE rnReceita <= 5

ORDER BY descCategoria, receita DESC

-- COMMAND ----------

-- 3. Qual é a Top 1 categoria de cada vendedor

WITH tb_vendedor_categoria AS (

  SELECT t1.idVendedor,
         t1.idProduto,
         t1.vlPreco,
         t2.descCategoria

  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

),
tb_vendedor AS (

  SELECT idVendedor,
         descCategoria,
         sum(vlPreco) AS receita
         
  FROM tb_vendedor_categoria

  WHERE descCategoria IS NOT null

  GROUP BY idVendedor, descCategoria

),
tb_final AS (

  SELECT *,
        row_number() OVER (PARTITION BY descCategoria ORDER BY receita DESC) AS rnReceita

  FROM tb_vendedor

)

SELECT *

FROM tb_final

WHERE rnReceita <= 1

ORDER BY descCategoria, receita DESC

-- COMMAND ----------

-- 4. Quais são as Top 2 categorias que mais vendem para clientes de cada estado?
WITH tb_pedido_categoria AS (

  SELECT t1.idCliente,
         t3.descCategoria,
         t2.idPedido

  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  INNER JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

),
tb_clienteUF AS (

  SELECT t1.descUF,
         t2.descCategoria,
         count(t2.idPedido) AS qtdPedidos

  FROM silver.olist.cliente AS t1

  INNER JOIN tb_pedido_categoria AS t2
  ON t1.idCliente = t2.idCliente

  GROUP BY t1.descUF, t2.descCategoria

),

tb_final AS (
  
  SELECT *,
        row_number() OVER (PARTITION BY descUF ORDER BY qtdPedidos DESC) AS rnPedidos
 
  FROM tb_clienteUF
 
  WHERE descCategoria IS NOT null
  
)

SELECT *

FROM tb_final

WHERE rnPedidos <= 2

ORDER BY descUF, qtdPedidos DESC 

-- COMMAND ----------

-- 5. Quantidade acumulada de itens vendidos por categoria ao longo do tempo.

WITH tb_data_categoria AS (

  SELECT date(t2.dtPedido) AS data,
         t3.descCategoria,
         count(*) AS qtdeItems

  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  INNER JOIN silver.olist.produto AS t3
  ON t1.idProduto = t3.idProduto

  WHERE t3.descCategoria IS NOT null

  GROUP BY data, t3.descCategoria

)

SELECT *,
       sum(qtdeItems) OVER (PARTITION BY descCategoria ORDER BY data) AS qtdeAcum

FROM tb_data_categoria

ORDER BY data, descCategoria

-- COMMAND ----------

-- 6. Receita acumulada por categoria ao longo do tempo

WITH tb_data_categoria AS (

  SELECT date(t2.dtPedido) AS data,
         t3.descCategoria,
         sum(t1.vlPreco) AS receita

  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  INNER JOIN silver.olist.produto AS t3
  ON t1.idProduto = t3.idProduto

  WHERE t3.descCategoria IS NOT null

  GROUP BY data, t3.descCategoria

)

SELECT *,
       sum(receita) OVER (PARTITION BY descCategoria ORDER BY data) AS receitaAcum

FROM tb_data_categoria

ORDER BY data, descCategoria

-- COMMAND ----------

-- 7. PLUS: Selecione um dia de venda aleatório de cada vendedor

WITH tb_data_vendedor AS(

  SELECT date(t1.dtPedido) AS data,
         t2.idVendedor

  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  ORDER BY data

),

tb_final AS (

  SELECT *,
         row_number() OVER (PARTITION BY idVendedor ORDER BY data) AS rnVenda

  FROM tb_data_vendedor

)

SELECT *

FROM tb_final

WHERE rnVenda <=1
