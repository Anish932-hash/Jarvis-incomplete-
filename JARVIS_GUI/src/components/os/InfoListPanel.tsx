import JarvisPanel from "./JarvisPanel";

interface InfoListPanelProps {
    title: string;
    items: string[];
}

const InfoListPanel = ({ title, items }: InfoListPanelProps) => {
    return (
        <JarvisPanel title={title} className="w-72">
            <ul className="space-y-0.5 text-xs text-foreground/80">
                {items.map((item, index) => (
                    <li key={index} className="flex items-center gap-2 truncate">
                        <div className="h-1 w-1 rounded-full bg-primary flex-shrink-0" />
                        <span className="truncate">{item}</span>
                    </li>
                ))}
            </ul>
        </JarvisPanel>
    )
}

export default InfoListPanel;
