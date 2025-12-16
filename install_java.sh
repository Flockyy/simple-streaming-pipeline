#!/bin/bash
# Installation de Java 11 pour Spark dans WSL

echo "📦 Installation de Java 11 JDK..."
sudo apt update && sudo apt install -y openjdk-11-jdk

echo ""
echo "✅ Vérification de l'installation..."
java -version

echo ""
echo "🔧 Configuration de JAVA_HOME..."
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo "JAVA_HOME=$JAVA_HOME"

# Ajouter à .zshrc
if ! grep -q "JAVA_HOME" ~/.zshrc 2>/dev/null; then
    echo "" >> ~/.zshrc
    echo "# Java pour Spark" >> ~/.zshrc
    echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.zshrc
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc
    echo "✅ JAVA_HOME ajouté à ~/.zshrc"
fi

echo ""
echo "🎉 Installation terminée !"
echo "Exécutez: source ~/.zshrc"
