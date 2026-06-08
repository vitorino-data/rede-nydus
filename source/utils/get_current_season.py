import requests


def get_current_season_id(token: str, region: str = 'us', region_id: int = 1) -> int:
    """
    Busca o season_id da season atual via SC2 Community API da Blizzard.

    Endpoint: GET /sc2/ladder/season/{regionId}
    regionId: 1=Americas, 2=Europe, 3=Korea

    Retorna o season_id como inteiro (ex: 67).
    """
    url = f"https://{region}.api.blizzard.com/sc2/ladder/season/{region_id}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    season_id = data.get('seasonId')
    if not season_id:
        raise ValueError(f"seasonId não encontrado na resposta da API: {data}")

    print(f"Season atual detectada: {season_id} (ano {data.get('year')}, número {data.get('number')})")
    return int(season_id)
