import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static java.lang.Math.pow;
import static java.util.Collections.min;

public class MyThread extends Thread{
    int id;
    int start;
    int end;

    public MyThread(int id) {
        this.id =  id;
    }

    public void run() {
        readArticles(id);

        try {
            Tema1.barrier.await();
        } catch(BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }

        if (id == 0) {
            /* Set initial number of articles. */
            Tema1.initialNrOfArticles = Tema1.articles.size();
        }
        /* TASK4 - Duplicate Removal */
        duplicateRemoval(id);

        try {
            Tema1.barrier.await();
        } catch(BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }

        /* TASK2 - Category Classification */
        categoryClassification(id);
        /* TASK3 - Language Classification */
        languageClassification(id);
        /* TASK5 - Interest Words */
        interestWords(id);

        try {
            Tema1.barrier.await();
        } catch(BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }

        /* Do some undone writing. */
        if (Tema1.NR_OF_THREADS == 1) {
            writeKeywordsCount();
            writeAllArticles();
            writeLanguages();
            writeCategories();
        } else if (Tema1.NR_OF_THREADS == 2) {
            if (id == 0) {
                writeAllArticles();
                writeLanguages();
            }
            if (id == 1) {
                writeKeywordsCount();
                writeCategories();
            }
        } else {
            if (id == 0) {
                writeAllArticles();
            }
            if (id == 1) {
                writeKeywordsCount();
            }
            if (id == 2) {
                writeCategories();
            }
            if (id == 3) {
                writeLanguages();
            }
        }

        try {
            Tema1.barrier.await();
        } catch(BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }


        /* TASK6 - Reports */
        bestAuthor(id);

        if (id == 0) {
            topLanguage();
            topCategory();
        }

        if (id == 0) {
            topEnglishWord();
        }

        mostRecentArticle(id);

        if (id == 0) {
            writeReports();
        }
    }

    public void readArticles(int id) {
        int start = id * (int) Math.ceil((double) Tema1.fileNames.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.fileNames.size(), (id + 1) * (int) Math.ceil((double) Tema1.fileNames.size() / Tema1.NR_OF_THREADS));

        for (int i = start; i < end; i++) {
            try {
                /* We initially used Article[] aux only to respect json format. */
                Article[] aux = Tema1.mapper.readValue(new File(Tema1.helperPath + Tema1.fileNames.get(i)), Article[].class);

                for (Article article : aux) {
                    Tema1.partialArticles.get(id).add(article);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            Tema1.barrier.await();
        } catch (BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }

        if (id == 0) {
            for (int i = 0; i < Tema1.NR_OF_THREADS; i++) {
                Tema1.articles.addAll(Tema1.partialArticles.get(i));
            }
        }

        Tema1.initialNrOfArticles = Tema1.articles.size();
    }

    public void writeLanguages() {

        /* Write into files. */
        for (String language : Tema1.languages) {
            ArrayList<String> languageResults = Tema1.articlesByLanguage.get(language);

            if (!languageResults.isEmpty()) {
                /* Order the articles inside languages. */
                Collections.sort(languageResults);

                try {
                    FileWriter fw = new FileWriter(language + ".txt");

                    for (String uuid : languageResults) {
                        fw.write(uuid + "\n");
                    }

                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeCategories() {
        for (String category : Tema1.categories) {
            ArrayList<String> categoryResults = Tema1.articlesByCategory.get(category);

            if (categoryResults != null && !categoryResults.isEmpty()) {
                /* Keep lexicographic order. */
                Collections.sort(categoryResults);

                try {
                    FileWriter fw = new FileWriter(normalizeCategoryName(category, true));

                    for (String uuid : categoryResults) {
                        fw.write(uuid + "\n");
                    }

                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void duplicateRemoval(int id) {
        /* Compute indexes. */
        int start = id * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.articles.size(), (id + 1) * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS));

        /* Iterate through assigned articles. */
        for (int i = start; i < end; i++) {
            Article article = Tema1.articles.get(i);

            Tema1.partialArticleByTitle.get(id).merge(article.title, 1, Integer::sum);
            Tema1.partialArticleByUuid.get(id).merge(article.uuid, 1, Integer::sum);
        }
        /* Only thread 0 merges partial results into global results. */

        for (Map.Entry<String, Integer> entry : Tema1.partialArticleByTitle.get(id).entrySet()) {
            Tema1.articleFreqByTitle.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }

        for (Map.Entry<String, Integer> entry : Tema1.partialArticleByUuid.get(id).entrySet()) {
            Tema1.articleFreqByUuid.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }

        /* Wait for all threads to finish partial work. */
        try {
            Tema1.barrier.await();
        } catch(BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }

        if (id == 0) {
            /* Remove duplicates. */
            for (Map.Entry<String, Integer> entry : Tema1.articleFreqByTitle.entrySet()) {
                if (entry.getValue() > 1) {
                    Tema1.articles.removeIf(element -> element.title.equals(entry.getKey()));
                }
            }

            for (Map.Entry<String, Integer> entry : Tema1.articleFreqByUuid.entrySet()) {
                if (entry.getValue() > 1) {
                    Tema1.articles.removeIf(element -> element.uuid.equals(entry.getKey()));
                }
            }

            /* Now that articles do not contain duplicates, make englishArticles. */
            for (Article article : Tema1.articles) {
                if (article.language.compareTo("english") == 0) {
                    Tema1.englishArticles.add(article);
                }
            }

            /* Compute statistics. */
            Tema1.nrOfUniqueArticles = Tema1.articles.size();
            Tema1.nrOfDuplicates = Tema1.initialNrOfArticles - Tema1.articles.size();
        }
    }

    public void topEnglishWord() {
        Tema1.topEnglishWord = Tema1.wordCount.get(0).getKey();
        Tema1.topEnglishWordCount = Tema1.wordCount.get(0).getValue();
    }

    public void writeReports() {
        /* Search for global most recent article. */
        for (Article article : Tema1.mostRecentArticleFromThreads) {
            /* If there is no most recent article set, just add current article. */
            if (Tema1.mostRecentArticle == null) {
                Tema1.mostRecentArticle = article;
            } else {
                /* Keep the more recent article. */
                if (Tema1.mostRecentArticle.published.compareTo(article.published) < 0) {
                    Tema1.mostRecentArticle = article;
                }
                else if (Tema1.mostRecentArticle.published.compareTo(article.published) == 0) {
                    /* TIEBREAKER */
                    if (Tema1.mostRecentArticle.uuid.compareTo(article.uuid) > 0) {
                        Tema1.mostRecentArticle = article;
                    }
                }
            }
        }

        /* Search for global best author. */
        for (int i = 0; i < Tema1.NR_OF_THREADS; i++) {
            for (Map.Entry<String, Integer> entry : Tema1.partialAuthorFreq.get(i).entrySet()) {
                Tema1.authorFreq.merge(entry.getKey(), entry.getValue(), Integer::sum);

                if (Tema1.bestAuthorCount < Tema1.authorFreq.get(entry.getKey())) {
                    Tema1.bestAuthor = entry.getKey();
                    Tema1.bestAuthorCount = Tema1.authorFreq.get(entry.getKey());
                }
            }
        }

        try {
            FileWriter fw = new FileWriter("reports.txt");

            fw.write("duplicates_found - " + Tema1.nrOfDuplicates + "\n");
            fw.write("unique_articles - " + Tema1.nrOfUniqueArticles + "\n");
            fw.write("best_author - " + Tema1.bestAuthor + " " + Tema1.bestAuthorCount + "\n");
            fw.write("top_language - " + Tema1.topLanguage + " " + Tema1.topLanguageCount + "\n");
            fw.write("top_category - " + normalizeCategoryName(Tema1.topCategory, false) + " " + Tema1.topCategoryCount + "\n");
            fw.write("most_recent_article - " + Tema1.mostRecentArticle.published + " " + Tema1.mostRecentArticle.url + "\n");
            fw.write("top_keyword_en - " + Tema1.topEnglishWord + " " + Tema1.topEnglishWordCount + "\n");

            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mostRecentArticle(int id) {
        /* Scatter articles. */

        /* Compute indexes. */
        int start = id * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.articles.size(), (id + 1) * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS));

        /* Iterate through all assigned articles. */
        for (int i = start; i < end; i++) {
            Article article = Tema1.articles.get(i);

            /* If local most recent article doesn't exist yet, just add the article. */
            if (Tema1.mostRecentArticleFromThreads.get(id) == null) {
                Tema1.mostRecentArticleFromThreads.set(id, article);
            }
            else {
                /* If local most recent article is older than current article, save current article. */
                if (Tema1.mostRecentArticleFromThreads.get(id).published.compareTo(article.published) < 0) {
                    Tema1.mostRecentArticleFromThreads.set(id, article);
                } else if (Tema1.mostRecentArticleFromThreads.get(id).published.compareTo(article.published) == 0) {
                    /* TIEBREAKER - uuid (lexicographic order) */
                    if (Tema1.mostRecentArticleFromThreads.get(id).uuid.compareTo(article.uuid) > 0) {
                        Tema1.mostRecentArticleFromThreads.set(id, article);
                    }
                }
            }
        }
    }

    public void topCategory() {
        for (Map.Entry<String, ArrayList<String>> entry : Tema1.articlesByCategory.entrySet()) {
            if (Tema1.topCategoryCount < entry.getValue().size()) {
                Tema1.topCategory = entry.getKey();
                Tema1.topCategoryCount = entry.getValue().size();
            }
        }
    }

    public void bestAuthor(int id) {
        /* Scatter articles. */

        /* Compute indexes. */
        int start = id * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.articles.size(), (id + 1) * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS));

        /* Iterate through assigned articles and increment authors' filed in authorFreq. */
        for (int i = start; i < end; i++) {
            String author = Tema1.articles.get(i).author;

            Tema1.partialAuthorFreq.get(id).merge(author, 1, Integer::sum);
        }
    }

    public void topLanguage() {
        /* Iterate through results of articlesByLanguage. */
        for (Map.Entry<String, ArrayList<String>> entry : Tema1.articlesByLanguage.entrySet()) {
            if (Tema1.topLanguageCount < entry.getValue().size()) {
                Tema1.topLanguage = entry.getKey();
                Tema1.topLanguageCount = entry.getValue().size();
            }
        }
    }

    public void writeKeywordsCount() {
        for (Map.Entry<String, Integer> entry : Tema1.globalWordCount.entrySet()) {
            Tema1.wordCount.add(entry);
        }

        Tema1.wordCount.sort(new EntryComparator());

        try {
            FileWriter fw = new FileWriter("keywords_count.txt");

            for (Map.Entry<String, Integer> entry : Tema1.wordCount) {
                fw.write(entry.getKey() + " " + entry.getValue() + "\n");
            }

            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeAllArticles() {
        try {
            FileWriter fw = new FileWriter("all_articles.txt");

            /* Order the articles in descending chronological order. */
            Collections.sort(Tema1.articles, new ArticleComparator());

            /* Write the ordered article uuids into the "all_articles.txt" file. */
            for (Article article : Tema1.articles) {
                String line = article.uuid + " " + article.published + "\n";

                fw.write(line);
            }

            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void interestWords(int id) {
        /* Scatter english articles. */

        /* Compute indexes. */
        int start = id * (int) Math.ceil((double) Tema1.englishArticles.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.englishArticles.size(), (id + 1) * (int) Math.ceil((double) Tema1.englishArticles.size() / Tema1.NR_OF_THREADS));

        /* Process all assigned articles. */
        for (int i = start; i < end; i++) {
            String text = Tema1.englishArticles.get(i).text.toLowerCase();
            String[] words = text.split("\\s+");
            Set<String> markedWords = new HashSet<>();

            for (String word : words) {
                /* Remove non-letter chars. */
                word = word.replaceAll("[^a-z]", "");

                if (word.isEmpty()) {
                    continue;
                }

                /* This if-clause prevents us from counting the nr of appearances of a word,
                 * instead of the nr of articles in which it appears. */
                if (!markedWords.contains(word) && !Tema1.englishLinkingWords.contains(word)) {
                    /* If word doesn't exist in dictionary, add it with value 1. */
                    /* Else, increment the value. */
                    Tema1.partialWordCount.get(id).merge(word, 1, Integer::sum);

                    markedWords.add(word);
                }
            }
        }

        for (Map.Entry<String, Integer> entry : Tema1.partialWordCount.get(id).entrySet()) {
            Tema1.globalWordCount.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
    }

    public void languageClassification(int id) {
        /* Compute indexes. */
        int start = id * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.articles.size(), (id + 1) * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS));

        /* Iterate through assigned articles. */
        for (int i = start; i < end; i++) {
            Article currentArticle = Tema1.articles.get(i);

            if (Tema1.languages.contains(currentArticle.language)) {
                Tema1.partialArticlesByLanguage.get(id).compute(currentArticle.language, (lang, list) -> {
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    if (!list.contains(currentArticle.uuid)) {
                        list.add(currentArticle.uuid);
                    }
                    return list;
                });
            }
        }

        /* Wait for all threads to finish execution before centralizing results into files. */
        for (Map.Entry<String, ArrayList<String>> entry : Tema1.partialArticlesByLanguage.get(id).entrySet()) {
            Tema1.articlesByLanguage.compute(entry.getKey(), (language, list) ->
            {list.addAll(entry.getValue());
                return list;});
        }
    }

    public void categoryClassification(int id) {
        /* Compute indexes. */
        int start = id * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS);
        int end = Math.min(Tema1.articles.size(), (id + 1) * (int) Math.ceil((double) Tema1.articles.size() / Tema1.NR_OF_THREADS));

        /* Iterate through the assigned section of articles. */
        for (int i = start; i < end; i++) {
            /* Iterate through article's categories. */
            Article currentArticle = Tema1.articles.get(i);

            for (String category : currentArticle.categories) {
                if (Tema1.categories.contains(category)) {
                    Tema1.partialArticlesByCategory.get(id).compute(category, (cat, list) -> {
                        if (list == null) {
                            list = new ArrayList<>();
                        }
                        if (!list.contains(currentArticle.uuid)) {
                            list.add(currentArticle.uuid);
                        }
                        return list;
                    });
                }
            }
        }

        for (Map.Entry<String, ArrayList<String>> entry : Tema1.partialArticlesByCategory.get(id).entrySet()) {
            Tema1.articlesByCategory.compute(entry.getKey(), (language, list) ->
            {list.addAll(entry.getValue());
                return list;});
        }
    }

    public String normalizeCategoryName(String category, boolean isFile) {
        StringBuffer fileName = new StringBuffer();

        for (char c : category.toCharArray()) {
            /* If char is a coma, step over it. */
            if (c == ',') {
                continue;
            }
            /*If char is blank space, replace it with '_'.  */
            else if (c == ' ') {
                fileName.append('_');
            } else {
                fileName.append(c);
            }
        }

        if (isFile) {
            fileName.append(".txt");
        }

        return fileName.toString();
    }
}
