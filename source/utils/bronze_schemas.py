"""
Validação de schema para payloads da API Blizzard (camada Bronze).

Garante que os campos críticos usados pelas transformações Silver existam
no payload antes de salvar o arquivo. Falha rápido com mensagem descritiva
em vez de propagar dados corrompidos para a Silver.

Estratégia de uso:
- Para dados de liga/estrutura (críticos): raise diretamente — falha a task.
- Para dados de ladders/partidas individuais: o chamador deve capturar e
  logar o erro, continuando com os demais itens (fail-fast por item).
"""


def validate_league_response(data: dict, league_id: int) -> None:
    """
    Valida o payload do endpoint de estrutura de liga.
    Campos obrigatórios: key.{season_id, queue_id, league_id, team_type},
    tier[].division[].ladder_id.
    """
    if not isinstance(data, dict):
        raise ValueError(
            f"Liga {league_id}: payload não é um dict (recebido {type(data).__name__})"
        )

    key = data.get('key')
    if not isinstance(key, dict):
        raise ValueError(f"Liga {league_id}: campo 'key' ausente ou inválido")

    for field in ('season_id', 'queue_id', 'league_id', 'team_type'):
        if field not in key:
            raise ValueError(f"Liga {league_id}: 'key.{field}' ausente")

    tiers = data.get('tier')
    if not isinstance(tiers, list):
        raise ValueError(f"Liga {league_id}: campo 'tier' ausente ou não é lista")

    for i, tier in enumerate(tiers):
        divisions = tier.get('division')
        if not isinstance(divisions, list):
            raise ValueError(f"Liga {league_id}: tier[{i}] sem campo 'division'")
        for j, division in enumerate(divisions):
            if 'ladder_id' not in division:
                raise ValueError(
                    f"Liga {league_id}: tier[{i}].division[{j}] sem 'ladder_id'"
                )


def validate_modern_ladder_response(data: dict, ladder_id: int) -> None:
    """
    Valida o payload do endpoint de ladder moderna (dados de MMR).
    Campo obrigatório: team (lista).
    """
    if not isinstance(data, dict):
        raise ValueError(
            f"Ladder moderno {ladder_id}: payload não é um dict (recebido {type(data).__name__})"
        )

    teams = data.get('team')
    if not isinstance(teams, list):
        raise ValueError(
            f"Ladder moderno {ladder_id}: campo 'team' ausente ou não é lista"
        )


def validate_legacy_ladder_response(data: dict, ladder_id: int) -> None:
    """
    Valida o payload do endpoint de ladder legada (clãs, nomes).
    Campo obrigatório: ladderMembers (lista).
    """
    if not isinstance(data, dict):
        raise ValueError(
            f"Ladder legacy {ladder_id}: payload não é um dict (recebido {type(data).__name__})"
        )

    members = data.get('ladderMembers')
    if not isinstance(members, list):
        raise ValueError(
            f"Ladder legacy {ladder_id}: campo 'ladderMembers' ausente ou não é lista"
        )


def validate_match_history_response(data: dict, profile_id: int) -> None:
    """
    Valida o payload do endpoint de histórico de partidas.
    Campo obrigatório: matches (lista).
    """
    if not isinstance(data, dict):
        raise ValueError(
            f"Match history jogador {profile_id}: payload não é um dict (recebido {type(data).__name__})"
        )

    matches = data.get('matches')
    if not isinstance(matches, list):
        raise ValueError(
            f"Match history jogador {profile_id}: campo 'matches' ausente ou não é lista"
        )
