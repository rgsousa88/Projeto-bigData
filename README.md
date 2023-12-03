# Projeto Interdisciplinar - Disciplina Tecnologias de Big Data
Este é o repositório do projeto final da disciplina Tecnologias de Big Data do curso de pós-graduação em Ciência de Dados do Instituto Federal de São Paulo - Campus Campinas.
O projeto propõe o desenvolvimento de uma solução completa (end-to-end) de dados usando ferramentas da AWS.

**Tema do projeto:** Engenharia de dados no apoio à tomada de decisão em processo de inventário.

**Alunos:** Lucas da Silva Lima (CP3021882) e Ridley Gadelha de Sousa (CP3021921)

**Requisitos de projeto:** [*Descrição dos requisitos*](./Requisitos) 

# Sumário

* [Introdução](#intro)
* [Descrição dos dados](#descri)
* [Workflow](#work)
* [Infraestrutura](#infra)
* [Conclusão](#conclu)
* [Trabalhos futuros](#fut)

# Introdução <a id='intro'></a>

## Processo de Inventário Dentro da Cadeia Logística
O processo de inventário é um dos pilares da cadeia logística, pois permite com que as empresas tenham um controle efetivo sobre seus estoques, identificando possíveis perdas e garantindo uma boa saúde dos ativos. Para auxiliar neste processo, é notória a importância da utilização de dados para uma melhor tomada decisão, uma vez que permite com que as empresas tenham uma visão mais clara e precisa de sua operação, identificando pontos de melhoria e oportunidades de otimização. 

## Serviços computacionais Cloud-Based
A revolução das ferramentas de nuvem tem sido um fator chave na transformação digital das empresas, permitindo que elas acessem serviços computacionais de forma mais ágil e eficiente, reduzindo custos e aumentando a eficiência operacional.Neste trabalho acadêmico, exploraremos a importância do processo de inventário dentro da cadeia logística, a importância da utilização de dados logísticos para a tomada de decisão e a revolução das ferramentas de nuvem na utilização de serviços computacionais, destacando as principais tendências e desafios enfrentados pelas empresas na era digital. Dessa forma, as empresas podem armazenar e processar grandes volumes de dados logísticos de forma mais eficiente e segura, permitindo que elas tenham uma visão mais clara e precisa de sua operação.

## Amazon Web Services (AWS)
A Amazon Web Services (AWS) é uma plataforma de computação em nuvem que oferece uma ampla variedade de serviços globais baseados em nuvem, incluindo computação, armazenamento, bancos de dados, análise, redes, dispositivos móveis, ferramentas de desenvolvimento, ferramentas de gerenciamento, IoT, segurança e aplicativos empresariais. A AWS fornece mais de 200 serviços, como data warehousing, ferramentas de implantação, diretórios e entrega de conteúdo, que podem ser provisionados rapidamente, sem despesas de capital inicial, permitindo que empresas de todos os tamanhos tenham acesso aos componentes básicos necessários para atender rapidamente às necessidades dinâmicas dos negócios [*1*](https://docs.aws.amazon.com/pt_br/whitepapers/latest/aws-overview/introduction.html)

# Descrição dos dados <a id='descri'></a> 

Os dados utilizados no projeto são dados reais, em ambiente de produção de uma companhia logística localizada em São Paulo. Todos os dados estão em conformidade com a Lei Geral de Proteção de Dados (LGPD), que estabelece regras sobre coleta, armazenamento, tratamento e compartilhamento de dados pessoais, inclusive na internet [*2*](https://www.gov.br/pt-br/temas/lgpd). Além disso a companhia em questão tomou os devidos cuidados para não expor nenhum dado confidencial estratégico do estoque.

**1. CBA030.csv:** Esta é a base principal do projeto. Trata-se de uma tabela utilizada como mestre de inventário. Trata-se de uma tabela com 88769 registros. A tabela a seguir, indica a relação das colunas presentes na tabela CBA030.

| Campo  | Título  | Descrição  |
| ------------ | ------------ | ------------ |
| CBA_FILIAL | Filial  |  Filial |
| CBA_CODINV | Cod.Invent. | Código do inventario | 
|  CBA_ANALIS | Análise | Análise |
| CBA_DATA  | Data  | Data |
| CBA_CONTS  | Contagens |  Contagens a realizar	|
| CBA_LOCAL  |  Almoxarifado | Almoxarifado |
| CBA_TIPINV | Tipo |  Tipo	 do Inventário |
| CBA_LOCALI | Endereco  | Endereço |
| CBA_PROD | Produto  | Código de Produto |
| CBA_CONTR |  Cont. Realiz | Contagens Realizadas |
| CBA_STATUS | Status | Status de contagem  |
| CBA_AUTREC | Recontagem | Autoriza Recontagem |
| CBA_CLASSA | Classe A | Classe A da Curva ABC	 |
| CBA_CLASSB | Classe B | Classe B da Curva ABC	 |
| CBA_CLASSC | Classe C | Classe C da Curva ABC	 |
| CBA_INVGUI | Inv. Guiado  | Inventário Guiado  |
| CBA_RECINV | Rec. Inv Mob	| Recebimento inventario Mobile |
| CBA_DISPOS | Dispositivo  | Dipositivo para coleta |


**2. CBB030.csv:** Cabeçalho de Inventário. Nesta tabela é possível consultar quais usuários realizaram cada uma das contagens e o status de cada uma. A Tabela a seguir indica os campos e suas respectivas descrições, presentes nesta tabela.
| Campo  | Título  | Descrição  |
| ------------ | ------------ | ------------ |
| CBB_FILIAL | Filial |  Filial |
| CBB_NUM | Número | Número do cabeçalho | 
|  CBB_CODINV | Codigo Inv. | Código do inventario |
| CBB_USU | Usuário | Código de usuário |
| CBB_NCONT | Nr. contagem |  Número da contagem	|
| CBB_STATUS | Status | Status da contagem |


**3. CBC030.csv:** Itens de Inventário. Esta é a tabela complementar à tabela CBB030. Com ela, é possível obter mais detalhes em cada uma das contagens realizadas pelos usuários como por exemplo a quantidade que foi efetivamente contada por cada um dos usuários, o produto que estava naquele endereço e também o lote do produto. A Tabela a seguir indica os campos e suas respectivas descrições, presentes nesta tabela.

| Campo  | Título  | Descrição  |
| ------------ | ------------ | ------------ |
| CBC_FILIAL | Filial  |  Filial |
| CBC_CODINV | Cod.Invent. | Código do inventario | 
| CBC_NUM | Número | Número do cabeçalho |
| CBC_COD | Produto | Código de Produto|
| CBC_LOCAL | Armazem |  Código de Armazém	|
| CBC_QUANT  |  Quantidade | Quantidade contada |
| CBC_QTDORI | Qtd.Original |  Quantidade Original |
| CBC_LOCALI| Endereco  | Endereço |
| CBC_LOTECT | Lote  | Lote |
| CBC_NUMLOT |  Sub-lote | Sub-lote |
| CBC_NUMSER | Num. serie | Número de série  |
| CBC_CODETI | Etiqueta | Código de etiqueta |
| CBC_CONTOK | Contagem OK | Contagem Ok	 |
| CBC_AJUST | Ajustado | Ajustado	 |
| CBC_AJUST | ID Unitiz | ID do Unitizado	 |
| CBC_CODUNI | Tipo Unitiz | Tipo do Unitizado  |


**4. Usuários.csv:** Tabela Dimensão com informações complementares como o nome dos usuários que realizaram as contagens. A Tabela a seguir indica os campos e suas respectivas descrições, presentes nesta tabela.
| Campo  | Título  | Descrição  |
| ------------ | ------------ | ------------ |
| USR_ID | ID de usuário |  ID de usuário |
| USR_CODIGO | Código de usuário | Código sistêmico  |de usuário | 
| USR_NOME | Nome Usr | Nome de usuário |

A tabela abaixo indica todos os relacionamentos que podem ser realizados entre as tabelas referenciadas acima, bem como quais as chaves utilizadas em cada relacionamento.

| Tabela Origem  | Tabela Destino  | Chave Origem  | Chave Destino | Cardinalidade |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| CBA (Mestre de inventario) | CBB (Cabecalho do inventario) |  CBA_CODINV | CBB_CODINV | 1 para N |
| CBB (Cabecalho do inventario) | Usuários | CBB_USU | USR_ID | N para 1 |
| CBB (Cabecalho do inventario) | CBC (Itens do inventario) | CBB_NUM | CBC+NUM | 1 para N |

# Workflow <a id='work'></a>
A imagem a seguir indica os recursos da AWS que foram utilizados, bem como a ordem de execução de cada um deles.

# Infraestrutura <a id='infra'></a>

# Conclusão <a id='conclu'></a>
Diante da análise aprofundada sobre as ferramentas oferecidas pela Amazon Web Services (AWS) para a resolução de desafios na área de engenharia de dados, é evidente que esta plataforma se destaca como uma escolha robusta e completa para a implementação de soluções em projetos dessa natureza. Ao longo desta pesquisa, foi possível constatar a amplitude e eficácia das ferramentas disponíveis na AWS, as quais se mostraram capazes de lidar com a complexidade inerente à engenharia de dados.

Um dos aspectos mais notáveis é a acessibilidade proporcionada pela AWS, que permite a execução de tarefas complexas sem a necessidade de um conhecimento extremamente detalhado. Isso se revela como uma vantagem significativa, pois democratiza o acesso a tecnologias avançadas, possibilitando que profissionais de diferentes níveis de expertise possam aproveitar plenamente os recursos oferecidos pela plataforma.

No contexto específico de um problema relacionado a um processo logístico, a AWS demonstrou ser uma ferramenta não apenas robusta, mas também altamente performática. A extração e transformação dos dados do projeto foram realizadas de maneira eficiente, atendendo às demandas específicas do cenário logístico. Essa capacidade de adaptação e eficácia no enfrentamento de desafios práticos reforça a posição da AWS como uma escolha sólida para profissionais e organizações que buscam soluções confiáveis na área de engenharia de dados.

# Trabalhos futuros <a id='fut'></a>

