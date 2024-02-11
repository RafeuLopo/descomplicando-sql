-- Databricks notebook source
-- 1.  Qual a nota (média, mínima e máxima) de cada vendedor que tiveram vendas no ano de 2017? E o percentual de pedidos avaliados com nota 5?
WITH tb_2017 (

    SELECT idPedido,
           dtPedido

    FROM silver.olist.pedido

    WHERE year(dtPedido) = '2017'
),
tb_vendedor_2017 (

    SELECT DISTINCT t1.idPedido,
           t1.idVendedor,
           t2.dtPedido

    FROM silver.olist.item_pedido AS t1

    INNER JOIN tb_2017 AS t2
    ON t1.idPedido = t2.idPedido
)

SELECT t2.idVendedor,
       min(t1.vlNota),
       avg(t1.vlNota),
       max(t1.vlNota),
       avg(CASE WHEN t1.vlNota = 5 THEN 1 ELSE 0 END) AS perc5

FROM silver.olist.avaliacao_pedido AS t1

INNER JOIN tb_vendedor_2017 AS t2
ON t1.idPedido = t2.idPedido

GROUP BY t2.idVendedor

-- COMMAND ----------

-- 2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30.

WITH tb_data (

    SELECT *

    FROM silver.olist.pedido

    WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'
),
tb_receita (

    SELECT t1.idVendedor,
           t1.idPedido,
           SUM(t1.vlPreco) as vlTotal

    FROM silver.olist.item_pedido AS t1

    INNER JOIN tb_data AS t2
    ON t1.idPedido = t2.idPedido

    GROUP BY t1.idVendedor, t1.idPedido

)

SELECT t1.idVendedor,
       min(t1.vlTotal),
       avg(t1.vlTotal),
       max(t1.vlTotal)

FROM tb_receita AS t1

INNER JOIN tb_data AS t2
ON t1.idPedido = t2.idPedido

GROUP BY t1.idVendedor

-- COMMAND ----------

-- 3. Calcule a quantidade de pedidos por meio de pagamento que cada vendedor teve em seus pedidos entre 2017-01-01 e 2017-06-30.

WITH tb_data (

  SELECT *

  FROM silver.olist.pedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'
),
tb_vendedor (
  SELECT DISTINCT t1.idVendedor,
         t1.idPedido

  FROM silver.olist.item_pedido AS t1

  INNER JOIN tb_data AS t2
  ON t1.idPedido = t2.idPedido
)

SELECT t2.idVendedor,
    count(CASE WHEN t1.descTipoPagamento = 'boleto' THEN 1 END) AS boleto_count,
    count(CASE WHEN t1.descTipoPagamento = 'credit_card' THEN 1 END) AS credit_count,
    count(CASE WHEN t1.descTipoPagamento = 'debit_card' THEN 1 END) AS debit_count,
    count(CASE WHEN t1.descTipoPagamento = 'voucher' THEN 1 END) AS voucher_count

FROM silver.olist.pagamento_pedido AS t1

INNER JOIN tb_vendedor AS t2
ON t1.idPedido = t2.idPedido

GROUP BY t2.idVendedor

-- COMMAND ----------

-- 4. Combine a query do exercício 2 e 3 de tal forma, que cada linha seja um vendedor, e que haja colunas para cada meio de pagamento (com a quantidade de pedidos) e as colunas das estatísticas do pedido do exercício 2 (média, maior valor e menor valor).

WITH tb_data (

  SELECT *

  FROM silver.olist.pedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'
),
tb_vendedor (
  SELECT DISTINCT t1.idVendedor,
         t1.idPedido

  FROM silver.olist.item_pedido AS t1

  INNER JOIN tb_data AS t2
  ON t1.idPedido = t2.idPedido
),
tb_tipo_pagamento (
  SELECT t2.idVendedor,
    count(CASE WHEN t1.descTipoPagamento = 'boleto' THEN 1 END) AS boleto_count,
    count(CASE WHEN t1.descTipoPagamento = 'credit_card' THEN 1 END) AS credit_count,
    count(CASE WHEN t1.descTipoPagamento = 'debit_card' THEN 1 END) AS debit_count,
    count(CASE WHEN t1.descTipoPagamento = 'voucher' THEN 1 END) AS voucher_count

    FROM silver.olist.pagamento_pedido AS t1

    INNER JOIN tb_vendedor AS t2
    ON t1.idPedido = t2.idPedido

    GROUP BY t2.idVendedor
),
tb_receita (

    SELECT t1.idVendedor,
           t1.idPedido,
           SUM(t1.vlPreco) as vlTotal

    FROM silver.olist.item_pedido AS t1

    INNER JOIN tb_data AS t2
    ON t1.idPedido = t2.idPedido

    GROUP BY t1.idVendedor, t1.idPedido

),
tb_avg_precos (
       SELECT t1.idVendedor,
       min(t1.vlTotal) AS minPreco,
       avg(t1.vlTotal) AS avgPreco,
       max(t1.vlTotal) AS maxPreco

FROM tb_receita AS t1

INNER JOIN tb_data AS t2
ON t1.idPedido = t2.idPedido

GROUP BY t1.idVendedor
)

SELECT t1.*,
       t2.minPreco,
       t2.avgPreco,
       t2.maxPreco

FROM tb_tipo_pagamento AS t1

INNER JOIN tb_avg_precos AS t2
ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido WHERE idVendedor = 'b37c4c02bda3161a7546a4e6d222d5b2'

-- COMMAND ----------


