from wordcloud import WordCloud
import matplotlib.pyplot as plt

def generate_wordcloud(words, output_file='wordcloud.png'):
    text = ' '.join(words)
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    wordcloud.to_file(output_file)

    print(f"[ANALYZE] WordCloud disimpan ke: {output_file}")

    # Tampilkan WordCloud (opsional)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.show()
