from urllib.parse import urlparse, parse_qs


def get_page_version_id(response):
    main_version_name = response.css('div.dropdown:nth-child(1) > a > span.bp3-button-text::text').get()
    release_date = response.css('div.dropdown:nth-child(2) > a > span.bp3-button-text::text').get()
    main_versions = list(zip(
        response.css('div.dropdown:nth-child(1) > div.bp3-menu > a::attr(href)').getall(),
        response.css('div.dropdown:nth-child(1) > div.bp3-menu > a::text').getall()
    ))
    minor_versions = list(zip(
        response.css('div.dropdown:nth-child(2) > div.bp3-menu > a::attr(href)').getall(),
        response.css('div.dropdown:nth-child(2) > div.bp3-menu > a::text').getall()
    ))
    for _, name in main_versions:
        for link, minor_version_release_date in minor_versions:
            if name == main_version_name and minor_version_release_date == release_date:
                return get_id_from_version_link(link)


def get_id_from_version_link(link):
    version_link = parse_qs(urlparse(link).query)['r'][0]
    return version_link
