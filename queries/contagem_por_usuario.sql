SELECT ID,
	NOME,
	COUNT(ID) as CONTAGEM
FROM(
		SELECT CBBTABLE.cbb_usu as ID,
			USERTABLE.usr_nome as NOME
		FROM "d2tec-inv-database"."cleaned-cbb" AS CBBTABLE
			INNER JOIN "d2tec-inv-database"."cleaned-usuario" as USERTABLE ON CBBTABLE.cbb_usu = USERTABLE.usr_id
	)
GROUP BY ID, NOME
ORDER BY CONTAGEM DESC
