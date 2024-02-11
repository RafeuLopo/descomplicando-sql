-- Databricks notebook source
SELECT * 

-- COMMAND ----------

SELECT idPedido,
        sum(vlFrete) AS totalFrete
FROM silver.olist.item_pedido
GROUP BY idPedido
ORDER BY totalFrete ASC

-- COMMAND ----------

SELECT idVendedor,
       count(DISTINCT idPedido) as QtdPedido
FROM silver.olist.item_pedido
GROUP BY idVendedor
ORDER BY QtdPedido DESC
LIMIT 1

-- COMMAND ----------

SELECT idVendedor, count(idProduto)
FROM silver.olist.item_pedido
GROUP BY idVendedor

-- COMMAND ----------

SELECT date(dtPedido) as dt, count(idPedido)
FROM silver.olist.pedido
GROUP BY dt

-- COMMAND ----------

SELECT DISTINCT descUF, count(DISTINCT idVendedor)
FROM silver.olist.vendedor
GROUP BY descUF

-- COMMAND ----------

SELECT DISTINCT descCidade, count(DISTINCT idVendedor)
FROM silver.olist.vendedor
GROUP BY descCidade

-- COMMAND ----------

SELECT DISTINCT descCategoria, count(idProduto)
FROM silver.olist.produto
WHERE descCategoria LIKE '%construcao%'
GROUP BY descCategoria

-- COMMAND ----------

SELECT count(idProduto)
FROM silver.olist.produto
WHERE descCategoria LIKE '%construcao%'

-- COMMAND ----------

SELECT sum(vlPreco) / count(DISTINCT idPedido) as avgValor, 
       sum(vlFrete) / count(DISTINCT idPedido) as avgFrete
FROM silver.olist.item_pedido

-- COMMAND ----------

SELECT *
FROM silver.olist.pagamento_pedido
WHERE descTipoPagamento = 'credit_card'

-- COMMAND ----------

SELECT sum(nrParcelas) / count(DISTINCT idPedido) as avgParcelas,
       sum(vlPagamento) / sum(nrParcelas) as avgPagamento
FROM silver.olist.pagamento_pedido
WHERE descTipoPagamento = 'credit_card'

-- COMMAND ----------

SELECT sum(date_diff(dtEntregue, dtAprovado)) / count(date_diff(dtEntregue, dtAprovado))
FROM silver.olist.pedido
WHERE date_diff(dtEntregue, dtAprovado) > 0 AND descSituacao = 'delivered'

-- COMMAND ----------

SELECT DISTINCT descCidade, count(DISTINCT idClienteUnico), count(DISTINCT idCliente)
FROM silver.olist.cliente
GROUP BY descCidade

-- COMMAND ----------

SELECT DISTINCT descCategoria, count(idProduto)
FROM silver.olist.produto
GROUP BY descCategoria

-- COMMAND ----------

SELECT DISTINCT descCategoria, count(idProduto), sum(vlPesoGramas) / count(idProduto)
FROM silver.olist.produto
GROUP BY descCategoria

-- COMMAND ----------

SELECT date(dtPedido) as diaPedido, 
       count(DISTINCT pedido.idPedido),
       sum(i.vlPreco) as vlReceita
FROM silver.olist.pedido
JOIN silver.olist.item_pedido AS i ON pedido.idPedido = i.idPedido
GROUP BY diaPedido
ORDER BY diaPedido

-- COMMAND ----------

SELECT idProduto,
       count(*) as qtdVenda,
       sum(vlPreco) as vlReceita
FROM silver.olist.item_pedido
GROUP BY idProduto
ORDER BY qtdVenda DESC
