# RD Data - versão multiarquivo

Estrutura do pacote:

- `config.py`: configuração geral, séries e parâmetros de logging.
- `logging_utils.py`: criação de logs por execução, log de erros e log rotativo opcional.
- `utils.py`: funções utilitárias de coleta, transformação e exportação.
- `validators.py`: validações automáticas da configuração, dados brutos, dados processados e abas finais.
- `collectors.py`: coleta de dados SGS, SIDRA e extração do RMD.
- `processors.py`: tratamentos e cálculos principais.
- `naming.py`: padronização final dos nomes das colunas exportadas.
- `exporters.py`: montagem das abas, exportação para Excel e resumo final da execução.
- `main.py`: ponto de entrada da aplicação.

## Como executar

No diretório acima da pasta `rd_data_multiarquivo`, execute:

```bash
python3 -m rd_data_multiarquivo.main
```

## Saídas esperadas

- Arquivo Excel: `Recent Developments Data.xlsx`
- Pasta de logs: `logs/`
  - log por execução
  - log de erros por execução
  - log rotativo opcional em `logs/current/`
