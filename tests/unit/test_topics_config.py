import yaml


def test_particoes():

    with open("config/topics.yaml", "r", encoding="utf-8") as arquivo:
        conteudo = arquivo.read()
        configuracao = yaml.safe_load(conteudo)
        topicos = configuracao["topics"]
        topico = topicos[0]
        partitions = topico["partitions"]
        assert partitions == 6
