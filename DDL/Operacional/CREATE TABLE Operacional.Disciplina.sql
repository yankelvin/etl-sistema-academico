CREATE TABLE Operacional.Disciplina(
	COD_DISC INT NOT NULL IDENTITY,
	COD_CURSO INT NOT NULL,
	NOM_DISC VARCHAR(100) NOT NULL,
	NUM_CREDITOS INT NOT NULL,
	NATUREZA CHAR NOT NULL
);

ALTER TABLE Operacional.Disciplina
   ADD CONSTRAINT PK_Disciplina_COD_DISC PRIMARY KEY CLUSTERED (COD_DISC);

ALTER TABLE Operacional.Disciplina
ADD CONSTRAINT COD_CURSO
FOREIGN KEY (COD_CURSO) REFERENCES Operacional.Curso(COD_CURSO);
