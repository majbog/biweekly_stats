import datetime
import feedparser
import pandas as pd
from requests import get
from bs4 import BeautifulSoup
from time import sleep


RSS_LINKS = [
    'https://www.dwutygodnik.com/rss/felietony/',
    'https://www.dwutygodnik.com/rss/artykuly/',
    'https://www.dwutygodnik.com/rss/krotko/'
]


class GetStats:

    def get_html_from_the_website(self, url):
        try:
            connection = get(url)
            data = connection.text
            soup = BeautifulSoup(data, 'html.parser')
            return soup
        except:
            print('connection error, let me try again')
            sleep(10)
            self.get_html_from_the_website(url)


    def get_tags_from_the_website(self, html):

        tags = html.find_all('div', class_='article-property__value--tags')
        tgs = ''
        for tag in tags:
            tgs += tag.text.replace('\t', '')
        tgs, tgs = tgs.replace('  ', ''), tgs.replace(', ', ',')
        if len(tgs) == 0:
            tgs = None
        else:
            tgs = tgs.split(',')
            tgs[0] = tgs[0][1:]
            tgs[-1] = tgs[-1][:-1]
        return tgs

    def get_subj_pub(self, html):
        subject = html.find_all('a', class_='u-unlink')[0]['href'][1:]
        if len(subject) == 0:
            subject = None
        return subject

    def separate_title_and_author(self, string):
        tit_auth = string
        title = string[:string.find('(') - 1]
        # need to reverse the string in order to find the last words put in ()
        author = tit_auth[::-1]
        author = author[1:author.find('(')][::-1]
        return author, title

    def get_data_articles(self):
        self.id_article = 1
        self.articles_metadata = []
        for link in RSS_LINKS:
            entries = feedparser.parse(link).entries
            for entry in entries:
                self.articles_metadata.append(entry)
        self.tags_articles_df = pd.DataFrame(columns=['link', 'tag'])
        self.articles_complete_info_df = pd.DataFrame(
            columns=['author', 'title', 'pub_date', 'subject', 'link']
        )
        for article in self.articles_metadata:
            author, title = self.separate_title_and_author(article['title'])
            art_link = article['link']
            published_date = datetime.datetime.strptime(article['published'][:-6], '%a, %d %b %Y %H:%M:%S')
            # enter website in order to scrap more detailed info
            art_html = self.get_html_from_the_website(art_link)
            if art_html: #ugly way to handle conn error atm
                tags = self.get_tags_from_the_website(art_html)
                if tags is not None:
                    for tag in tags:
                        self.tags_articles_df = self.tags_articles_df.append(
                            {'link': art_link, 'tag': tag},
                            ignore_index=True
                        )
                subject = self.get_subj_pub(art_html)
                if title and author and published_date and subject:
                    self.articles_complete_info_df = self.articles_complete_info_df.append({
                        'author': author,
                        'title': title,
                        'pub_date': published_date,
                        'subject': subject,
                        'link': art_link
                    }, ignore_index=True)
        ### temporary solution for the tests
        self.articles_complete_info_df.to_csv('./articles.csv')
        self.tags_articles_df.to_csv('./tags.csv')
        ###
        return self.tags_articles_df, self.articles_complete_info_df

    def scrap_through_editions(self):
        self.tags_articles_df = pd.DataFrame(columns=['link', 'tag'])
        self.articles_complete_info_df = pd.DataFrame(
            columns=['author', 'title', 'pub_date', 'subject', 'link']
        )
        entries = feedparser.parse('https://www.dwutygodnik.com/rss/wydanie').entries
        for edition in entries:
            parsed_html = BeautifulSoup(edition['summary'], 'html.parser')
            published_date = datetime.datetime.strptime(edition['published'][:-6], '%a, %d %b %Y %H:%M:%S')
            for article in parsed_html.find_all('a'):
                author, title = self.separate_title_and_author(article.text)
                link = article['href']
                art_html = self.get_html_from_the_website(link)
                subject = self.get_subj_pub(art_html)
                tags = self.get_tags_from_the_website(art_html)
                if tags is not None:
                    for tag in tags:
                        self.tags_articles_df = self.tags_articles_df.append(
                            {'link': link, 'tag': tag},
                            ignore_index=True
                        )
                if title and author and subject and published_date:
                    self.articles_complete_info_df = self.articles_complete_info_df.append({
                        'author': author,
                        'title': title,
                        'pub_date': published_date,
                        'subject': subject,
                        'link': link
                    }, ignore_index=True)
            return self.tags_articles_df, self.articles_complete_info_df

    def prepare_tables(self):
        self.general_tags_df = pd.DataFrame(
            columns=['link', 'tag']
        )
        direct_tag_df, direct_art_df = self.get_data_articles()
        tags_via_editions_df, art_via_editions_df = self.scrap_through_editions()
        self.general_art_df = pd.concat([direct_art_df, art_via_editions_df])
        self.general_tags_df = pd.concat([tags_via_editions_df, direct_tag_df])
        self.general_tags_df.drop_duplicates(keep='first')
        self.general_art_df.drop_duplicates(keep='first')
        return self.general_tags_df, self.general_art_df


    def prepare_stats(self):
        # temporary solution
        tags = pd.read_csv('tags.csv')
        articles = pd.read_csv('articles.csv')
        tags_by_year = self.organize_tags_by_year(tags, articles)
        return tags_by_year


    def organize_tags_by_year(self, df_tags, df_articles):
        dfs = pd.merge(df_articles, df_tags, how='left', on='link')
        dfs['pub_date'] = pd.to_datetime(dfs['pub_date'])
        dfs['pub_date'] = dfs['pub_date'].dt.year
        ty = dfs.loc[dfs.tag.notnull(), ['pub_date', 'tag']]
        ty = ty.groupby('pub_date').tag.value_counts()
        ty = ty.groupby(level=0).nlargest(5).reset_index(level=0, drop=True)
        ty_dict = {}
        year = []
        for i in ty.index:
            if i[0] not in year:
                year.append(i[0])
        for y in year:
            ty_dict[y] = []
            each_year = ty.loc[y,:].to_dict()
            for yr in each_year:
                dict = {yr[1]:each_year[yr]}
                ty_dict[y].append(dict)

        return ty_dict


if '__main__' == __name__:
    a = GetStats()
    print(a.prepare_stats())
