CREATE TABLE DW.DM_Curso(
	COD_CURSO INT NOT NULL IDENTITY,
	DESCRICAO VARCHAR(100) NOT NULL,
	NUM_CREDITOS INT NOT NULL,
	DURACAO INT NOT NULL,
	NOME_DEPARTAMENTO VARCHAR(100) NOT NULL
);

ALTER TABLE DW.DM_Curso
   ADD CONSTRAINT PK_DM_Curso_COD_CURSO PRIMARY KEY CLUSTERED (COD_CURSO);

ALTER TABLE DW.FT_Nota
ADD CONSTRAINT COD_CURSO
FOREIGN KEY (COD_CURSO) REFERENCES DW.DM_Curso(COD_CURSO);