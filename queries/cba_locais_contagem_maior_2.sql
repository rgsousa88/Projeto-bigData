SELECT INVTABLE.cba_codinv as CONDINV,
	INVTABLE.cba_locali_street as RUA,
	INVTABLE.cba_locali_build as PREDIO,
	INVTABLE.cba_locali_level as NIVEL,
	CONT as CONTAGEM
FROM(
		SELECT CBBTABLE.cbb_codinv as CODINV,
			COUNT(CBBTABLE.cbb_codinv) as CONT
		FROM "d2tec-inv-database"."cleaned-cbb" as CBBTABLE
		GROUP BY CBBTABLE.cbb_codinv
		ORDER BY CONT DESC
	)
	INNER JOIN "d2tec-inv-database"."cleaned-inventario" as INVTABLE ON INVTABLE.cba_codinv = CODINV
	WHERE YEAR(INVTABLE.cba_data) >= 2023 AND CONT > 2
	ORDER BY CONTAGEM DESC
