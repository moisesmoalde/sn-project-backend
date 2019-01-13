from scipy import stats

import neo4jUtils as db


THRES = 0.5


def main():
    setAllSimilarities("anonymous100")

def setAllSimilarities(email):
    "Sets similarity with all users with common liked movies"
    db.deleteSimilarities(email)
    userLikes = db.getLikesCount(email)
    others = db.getUsersWithCommonLikes(email)

    while others.forward():
        otherID = others.current[0]
        common = float(others.current[1])
        otherLikes = float(others.current[2])
        similarity = stats.hmean([common/userLikes, common/otherLikes])

        if similarity >= THRES:
            db.updateSimilarity(email, otherID, similarity)

if __name__ == '__main__':
    main()