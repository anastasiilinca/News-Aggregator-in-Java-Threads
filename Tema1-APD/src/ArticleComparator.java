import java.util.Comparator;

public class ArticleComparator implements Comparator<Article> {
    @Override
    public int compare(Article a1, Article a2) {

        /* If article1 was published before article2 => article2 has priority */
        if (a1.published.compareTo(a2.published) < 0) {
            return 1;
        }
        /* If article1 was published after article2 => article1 has priority */
        else if (a1.published.compareTo(a2.published) > 0) {
            return -1;
        }
        /* If articles were published at the exact same time, compare uuids. */
        else {
            /* If uuid1 comes first => uuid1 has priority. */
            if (a1.uuid.compareTo(a2.uuid) < 0) {
                return -1;
            } else if (a1.uuid.compareTo(a2.uuid) > 0) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
